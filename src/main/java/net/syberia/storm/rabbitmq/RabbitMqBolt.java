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

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author Andrey Burov
 */
@Slf4j
public class RabbitMqBolt extends BaseRichBolt {

    private static final long serialVersionUID = 377563808237437264L;

    public static final String KEY_MANDATORY = "rabbitmq.mandatory";
    public static final String KEY_IMMEDIATE = "rabbitmq.immediate";

    private final RabbitMqConfig rabbitMqConfig;
    private final TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter;

    private OutputCollector collector;

    private boolean mandatory;
    private boolean immediate;

    private RabbitMqChannelFactory rabbitMqChannelFactory;
    private Channel channel;
    
    public RabbitMqBolt(TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter) {
        this(null, tupleToRabbitMqMessageConverter);
    }

    public RabbitMqBolt(RabbitMqConfig rabbitMqConfig, TupleToRabbitMqMessageConverter tupleToRabbitMqMessageConverter) {
        this.rabbitMqConfig = rabbitMqConfig;
        this.tupleToRabbitMqMessageConverter = tupleToRabbitMqMessageConverter;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        mandatory = ConfigFetcher.fetchBooleanProperty(stormConf, KEY_MANDATORY, false);
        immediate = ConfigFetcher.fetchBooleanProperty(stormConf, KEY_IMMEDIATE, false);

        tupleToRabbitMqMessageConverter.prepare(stormConf, context);
        
        rabbitMqChannelFactory = createRabbitMqChannelFactory(stormConf);
        
        try {
            rabbitMqChannelFactory.prepare();
        } catch (IOException | TimeoutException ex) {
            throw new RuntimeException("Unable to prepare RabbitMQ channel factory", ex);
        }

        try {
            channel = rabbitMqChannelFactory.createChannel();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to create RabbitMQ channel from the factory", ex);
        }
    }

    RabbitMqChannelFactory createRabbitMqChannelFactory(Map stormConf) { // package-private for testing
        if (rabbitMqConfig == null) {
            return RabbitMqChannelFactory.withStormConfig(stormConf);
        } else {
            return RabbitMqChannelFactory.withRabbitMqConfig(rabbitMqConfig);
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
            rabbitMqChannelFactory.cleanup();
        } catch (Exception ex) {
            log.error("Unable to cleanup RabbitMQ channel factory", ex);
        }
        tupleToRabbitMqMessageConverter.cleanup();
    }

}
