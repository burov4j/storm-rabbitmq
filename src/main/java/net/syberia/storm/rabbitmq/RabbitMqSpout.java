/*
 * Copyright 2017 Andrey Burov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syberia.storm.rabbitmq;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqSpout extends BaseRichSpout {

    private static final long serialVersionUID = 614091429512483100L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqSpout.class);

    public static final String KEY_QUEUE_NAME = "rabbitmq.queue_name";
    public static final String KEY_PREFETCH_COUNT = "rabbitmq.prefetch_count";
    public static final String KEY_REQUEUE_ON_FAIL = "rabbitmq.requeue_on_fail";
    public static final String KEY_AUTO_ACK = "rabbitmq.auto_ack";

    private final RabbitMqMessageScheme rabbitMqMessageScheme;

    private RabbitMqChannelProvider rabbitMqChannelProvider;

    private RabbitMqInitializer initializer;

    private String queueName;
    private boolean requeueOnFail;
    private boolean autoAck;
    private SpoutOutputCollector collector;

    private Channel channel;
    AutorecoverableQueueingConsumer queueingConsumer; // package-private for testing

    private boolean active;

    public RabbitMqSpout(RabbitMqMessageScheme rabbitMqMessageScheme) {
        this(null, rabbitMqMessageScheme);
    }

    public RabbitMqSpout(RabbitMqChannelProvider rabbitMqChannelProvider,
            RabbitMqMessageScheme rabbitMqMessageScheme) {
        this.rabbitMqChannelProvider = rabbitMqChannelProvider;
        this.rabbitMqMessageScheme = rabbitMqMessageScheme;
    }

    public void setInitializer(RabbitMqInitializer initializer) {
        this.initializer = initializer;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.queueName = ConfigFetcher.fetchStringProperty(conf, KEY_QUEUE_NAME);
        this.requeueOnFail = ConfigFetcher.fetchBooleanProperty(conf, KEY_REQUEUE_ON_FAIL, false);
        this.autoAck = ConfigFetcher.fetchBooleanProperty(conf, KEY_AUTO_ACK, false);
        int prefetchCount = ConfigFetcher.fetchIntegerProperty(conf, KEY_PREFETCH_COUNT, 50);
        if (prefetchCount < 1) {
            throw new IllegalArgumentException("Invalid prefetch count: " + prefetchCount);
        }
        this.collector = collector;

        this.rabbitMqMessageScheme.prepare(conf, context);

        if (this.rabbitMqChannelProvider == null) {
            this.rabbitMqChannelProvider = RabbitMqChannelProvider.withStormConfig(conf);
        }

        try {
            this.rabbitMqChannelProvider.prepare();
        } catch (IOException | TimeoutException ex) {
            throw new RuntimeException("Unable to prepare RabbitMQ channel provider", ex);
        }

        try {
            channel = rabbitMqChannelProvider.getChannel();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to get RabbitMQ channel from the provider", ex);
        }

        if (initializer != null) {
            try {
                initializer.initialize(channel);
            } catch (IOException ex) {
                throw new RuntimeException("Unable to execute initialization", ex);
            }
        }

        queueingConsumer = new AutorecoverableQueueingConsumer(channel);

        try {
            channel.basicQos(prefetchCount);
        } catch (IOException ex) {
            throw new RuntimeException("Unable set quality of service", ex);
        }

        try {
            channel.basicConsume(queueName, autoAck, context.getThisComponentId(), queueingConsumer);
        } catch (IOException ex) {
            throw new RuntimeException("Unable to start consuming the queue", ex);
        }
    }

    @Override
    public void nextTuple() {
        if (!active) {
            return;
        }

        while (true) {
            RabbitMqMessage rabbitMqMessage;
            try {
                rabbitMqMessage = queueingConsumer.nextMessage(1L);
            } catch (InterruptedException ex) {
                LOGGER.info("The consumer interrupted");
                return;
            }

            if (rabbitMqMessage == null) {
                LOGGER.trace("There are no messages in the queue");
                return;
            }

            Envelope envelope = rabbitMqMessage.getEnvelope();
            long messageId = envelope.getDeliveryTag();

            StreamedTuple streamedTuple;
            try {
                streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(envelope,
                        rabbitMqMessage.getProperties(), rabbitMqMessage.getBody());
            } catch (Exception ex) {
                collector.reportError(ex);
                try {
                    channel.basicReject(messageId, false);
                } catch (IOException rejectEx) {
                    collector.reportError(rejectEx);
                }
                return;
            }

            if (streamedTuple == null) {
                LOGGER.trace("Filtered message with id: {}", messageId);
                try {
                    channel.basicAck(messageId, false);
                } catch (IOException ackEx) {
                    collector.reportError(ackEx);
                }
            } else {
                collector.emit(streamedTuple.getStreamId(), streamedTuple.getTuple(), messageId);
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        processMessageId(msgId, (long deliveryTag) -> {
            channel.basicAck(deliveryTag, false);
        });
    }

    @Override
    public void fail(Object msgId) {
        processMessageId(msgId, (long deliveryTag) -> {
            channel.basicReject(deliveryTag, requeueOnFail);
        });
    }

    private void processMessageId(Object msgId, ChannelAction channelAction) {
        if (autoAck) {
            return;
        }
        long deliveryTag = (long) msgId;
        try {
            channelAction.execute(deliveryTag);
        } catch (AlreadyClosedException ex) {
            LOGGER.debug("Unable to process message id: " + deliveryTag, ex);
        } catch (IOException ex) {
            collector.reportError(ex);
        }
    }

    private interface ChannelAction {

        void execute(long deliveryTag) throws IOException;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Map<String, Fields> streamsOutputFields = rabbitMqMessageScheme.getStreamsOutputFields();
        for (Entry<String, Fields> entry : streamsOutputFields.entrySet()) {
            declarer.declareStream(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void deactivate() {
        this.active = false;
    }

    @Override
    public void activate() {
        this.active = true;
    }

    @Override
    public void close() {
        try {
            this.rabbitMqChannelProvider.cleanup();
        } catch (AlreadyClosedException ex) {
            LOGGER.info("Connection is already closed");
        } catch (Exception ex) {
            LOGGER.error("Unable to cleanup RabbitMQ provider", ex);
        }
        this.rabbitMqMessageScheme.cleanup();
    }

}
