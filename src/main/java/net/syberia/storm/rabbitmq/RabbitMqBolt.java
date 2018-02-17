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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqBolt extends BaseRichBolt {

    private static final long serialVersionUID = 377563808237437264L;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqBolt.class);

    public static final String KEY_MANDATORY = "rabbitmq.mandatory";
    public static final String KEY_IMMEDIATE = "rabbitmq.immediate";

    private final TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter;
    
    private RabbitMqChannelProvider rabbitMqChannelProvider;
    private Channel channel;

    private boolean mandatory;
    private boolean immediate;

    private OutputCollector collector;
    
    public RabbitMqBolt(TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter) {
        this(null, tupleToRabbitMqMessageConverter);
    }

    public RabbitMqBolt(RabbitMqChannelProvider rabbitMqChannelProvider,
            TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter) {
        this.rabbitMqChannelProvider = rabbitMqChannelProvider;
        this.tupleToRabbitMqMessageConverter = tupleToRabbitMqMessageConverter;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.mandatory = ConfigFetcher.fetchBooleanProperty(stormConf, KEY_MANDATORY, false);
        this.immediate = ConfigFetcher.fetchBooleanProperty(stormConf, KEY_IMMEDIATE, false);

        this.collector = collector;

        this.tupleToRabbitMqMessageConverter.prepare(stormConf, context);
        
        if (this.rabbitMqChannelProvider == null) {
            this.rabbitMqChannelProvider = RabbitMqChannelProvider.withStormConfig(stormConf);
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
    }

    @Override
    public void execute(Tuple input) {
        String exchange, routingKey;
        AMQP.BasicProperties properties;
        byte[] messageBody;
        try {
            exchange = tupleToRabbitMqMessageConverter.getExchange(input);
            routingKey = tupleToRabbitMqMessageConverter.getRoutingKey(input);
            properties = tupleToRabbitMqMessageConverter.getProperties(input);
            messageBody = tupleToRabbitMqMessageConverter.getMessageBody(input);
        } catch (Exception ex) {
            collector.reportError(ex);
            collector.fail(input);
            return;
        }

        try {
            channel.basicPublish(exchange, routingKey, mandatory, immediate, properties, messageBody);
        } catch (IOException ex) {
            collector.reportError(ex);
            collector.fail(input);
            return;
        }
        
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no operation
    }

    @Override
    public void cleanup() {
        try {
            this.rabbitMqChannelProvider.cleanup();
        } catch (Exception ex) {
            LOGGER.error("Unable to cleanup RabbitMQ provider", ex);
        }
        this.tupleToRabbitMqMessageConverter.cleanup();
    }

}
