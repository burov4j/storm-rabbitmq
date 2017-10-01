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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
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
    public static final String KEY_EMPTY_QUEUE_SLEEP_MILLIS = "rabbitmq.empty_queue_sleep_millis";

    private final RabbitMqMessageScheme rabbitMqMessageScheme;
    
    private RabbitMqChannelProvider rabbitMqChannelProvider;

    private RabbitMqInitializer initializer;

    private String queueName;
    private int prefetchCount;
    private boolean requeueOnFail;
    private long emptyQueueSleepMillis;
    private SpoutOutputCollector collector;

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
        this.prefetchCount = ConfigFetcher.fetchIntegerProperty(conf, KEY_PREFETCH_COUNT, 10);
        this.requeueOnFail = ConfigFetcher.fetchBooleanProperty(conf, KEY_REQUEUE_ON_FAIL, false);
        this.emptyQueueSleepMillis = ConfigFetcher.fetchLongProperty(conf, KEY_EMPTY_QUEUE_SLEEP_MILLIS, 100L);
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

        if (initializer != null) {
            Channel channel = getChannel();
            if (channel != null) {
                try {
                    initializer.initialize(channel);
                } catch (IOException ex) {
                    throw new RuntimeException("Unable to execute initialization", ex);
                } finally {
                    rabbitMqChannelProvider.returnChannel(channel);
                }
            }
        }
    }

    @Override
    public void nextTuple() {
        if (!active) {
            return;
        }

        Channel channel = getChannel();
        if (channel == null) {
            return;
        }

        try {
            for (int emitted = 0; emitted < prefetchCount; emitted++) {
                GetResponse response = channel.basicGet(queueName, false);
                if (response == null) { // no message retrieved
                    Utils.sleep(emptyQueueSleepMillis);
                    return;
                } else {
                    long messageId = response.getEnvelope().getDeliveryTag();
                    StreamedTuple streamedTuple;
                    try {
                        streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(response);
                    } catch (Exception ex) {
                        LOGGER.error("Unable to convert RabbitMQ message", ex);
                        collector.reportError(ex);
                        channel.basicReject(messageId, false);
                        continue;
                    }
                    if (streamedTuple == null) {
                        LOGGER.debug("Filtered message with id: {}", messageId);
                        channel.basicAck(messageId, false);
                    } else {
                        collector.emit(streamedTuple.getStreamId(), streamedTuple.getTuple(), messageId);
                    }
                }
            }
        } catch (IOException ex) {
            LOGGER.error("Unable to execute channel command", ex);
            collector.reportError(ex);
        } finally {
            rabbitMqChannelProvider.returnChannel(channel);
        }
    }

    @Override
    public void ack(Object msgId) {
        processMessageId(msgId, (Channel channel, long deliveryTag) -> {
            channel.basicAck(deliveryTag, false);
        });
    }

    @Override
    public void fail(Object msgId) {
        processMessageId(msgId, (Channel channel, long deliveryTag) -> {
            channel.basicReject(deliveryTag, requeueOnFail);
        });
    }

    private void processMessageId(Object msgId, ChannelAction channelAction) {
        Channel channel = getChannel();
        if (channel == null) {
            return;
        }
        long deliveryTag = (long) msgId;
        try {
            channelAction.execute(channel, deliveryTag);
        } catch (IOException ex) {
            LOGGER.error("Unable to process message id: " + deliveryTag, ex);
            collector.reportError(ex);
        } finally {
            rabbitMqChannelProvider.returnChannel(channel);
        }
    }

    private interface ChannelAction {

        void execute(Channel channel, long deliveryTag) throws IOException;
    }

    private Channel getChannel() {
        try {
            return rabbitMqChannelProvider.getChannel();
        } catch (Exception ex) {
            LOGGER.error("Unable to get RabbitMQ channel from the provider", ex);
            collector.reportError(ex);
            return null;
        }
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
        } catch (Exception ex) {
            LOGGER.error("Unable to cleanup RabbitMQ provider", ex);
        }
        this.rabbitMqMessageScheme.cleanup();
    }

}
