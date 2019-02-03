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
package ru.burov4j.storm.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqBoltTest extends StormRabbitMqTest {

    private OutputCollector mockOutputCollector;

    @Before
    public void setUp() {
        mockOutputCollector = mock(OutputCollector.class);
    }

    @Test
    public void prepareAndUse() throws Exception {
        String exchange = "testExchange",
                routingKey = "testRoutingKey";
        byte[] messageBody = "testMessageBody".getBytes(StandardCharsets.UTF_8);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().build();
        RabbitMqBolt rabbitMqBolt = Mockito.spy(new RabbitMqBolt(new EmptyTupleToRabbitMqMessageConverter() {
            @Override
            public String getExchange(Tuple tuple) {
                return exchange;
            }

            @Override
            public String getRoutingKey(Tuple tuple) {
                return routingKey;
            }

            @Override
            public AMQP.BasicProperties getProperties(Tuple tuple) {
                return properties;
            }

            @Override
            public byte[] getMessageBody(Tuple tuple) {
                return messageBody;
            }
        }));

        rabbitMqBolt.declareOutputFields(null);

        Map<String, Object> stormConf = new HashMap<>(2);
        stormConf.put(RabbitMqBolt.KEY_MANDATORY, true);
        stormConf.put(RabbitMqBolt.KEY_IMMEDIATE, true);

        prepareMockRabbitMqChannelFactory(rabbitMqBolt);

        rabbitMqBolt.prepare(stormConf, null, mockOutputCollector);

        Tuple mockTuple = mock(Tuple.class);
        rabbitMqBolt.execute(mockTuple);
        rabbitMqBolt.cleanup();

        verify(mockChannel, times(1)).basicPublish(exchange, routingKey, true, true, properties, messageBody);
        verify(mockOutputCollector, times(1)).ack(mockTuple);
        verify(rabbitMqChannelFactory, times(1)).cleanup();
    }

    @Test(expected = RuntimeException.class)
    public void unableToPrepare() throws IOException, TimeoutException {
        doThrow(IOException.class).when(rabbitMqChannelFactory).prepare();
        createAndPrepareSimpleRabbitMqBolt();
    }

    @Test(expected = RuntimeException.class)
    public void unableToCreateChannel() throws Exception {
        doThrow(IOException.class).when(rabbitMqChannelFactory).createChannel();
        createAndPrepareSimpleRabbitMqBolt();
    }

    @Test
    public void unableToPublish() throws Exception {
        doThrow(IOException.class).when(mockChannel).basicPublish(any(), any(), eq(false), eq(false), any(), any());
        RabbitMqBolt rabbitMqBolt = createAndPrepareSimpleRabbitMqBolt();

        Tuple tuple = mock(Tuple.class);
        rabbitMqBolt.execute(tuple);

        verify(mockOutputCollector, times(1)).reportError(any(IOException.class));
        verify(mockOutputCollector, times(1)).fail(tuple);
    }

    @Test
    public void unableToGetMessageBody() {
        RabbitMqBolt rabbitMqBolt = Mockito.spy(new RabbitMqBolt(new EmptyTupleToRabbitMqMessageConverter() {
            @Override
            public byte[] getMessageBody(Tuple tuple) {
                throw new RuntimeException();
            }
        }));
        prepareMockRabbitMqChannelFactory(rabbitMqBolt);
        prepareSimpleRabbitMqBolt(rabbitMqBolt);

        Tuple tuple = mock(Tuple.class);
        rabbitMqBolt.execute(tuple);

        verify(mockOutputCollector, times(1)).reportError(any(RuntimeException.class));
        verify(mockOutputCollector, times(1)).fail(tuple);
    }

    @Test
    public void unableToCleanup() throws Exception {
        doThrow(IOException.class).when(rabbitMqChannelFactory).cleanup();
        RabbitMqBolt rabbitMqBolt = createAndPrepareSimpleRabbitMqBolt();
        rabbitMqBolt.cleanup();
    }

    @Test
    public void createRabbitMqChannelFactoryStormConf() {
        RabbitMqBolt rabbitMqBolt = createSimpleRabbitMqBolt();
        Map<String, Object> conf = Collections.singletonMap(RabbitMqConfig.KEY_USERNAME, "user1");
        RabbitMqChannelFactory channelFactory = rabbitMqBolt.createRabbitMqChannelFactory(conf);
        ConnectionFactory connectionFactory = channelFactory.createConnectionFactory();

        assertEquals("user1", connectionFactory.getUsername());
    }

    @Test
    public void createRabbitMqChannelFactoryRabbitMqConf() {
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setUsername("user1")
                .build();
        RabbitMqBolt rabbitMqBolt = createSimpleRabbitMqBolt(rabbitMqConfig);
        Map<String, Object> conf = Collections.singletonMap(RabbitMqConfig.KEY_USERNAME, "user2");
        RabbitMqChannelFactory channelFactory = rabbitMqBolt.createRabbitMqChannelFactory(conf);
        ConnectionFactory connectionFactory = channelFactory.createConnectionFactory();

        assertEquals("user1", connectionFactory.getUsername());
    }

    private RabbitMqBolt createAndPrepareSimpleRabbitMqBolt() {
        RabbitMqBolt rabbitMqBolt = createSimpleRabbitMqBolt();
        prepareMockRabbitMqChannelFactory(rabbitMqBolt);
        prepareSimpleRabbitMqBolt(rabbitMqBolt);
        return rabbitMqBolt;
    }

    private RabbitMqBolt createSimpleRabbitMqBolt() {
        return Mockito.spy(new RabbitMqBolt(new EmptyTupleToRabbitMqMessageConverter()));
    }

    private RabbitMqBolt createSimpleRabbitMqBolt(RabbitMqConfig rabbitMqConfig) {
        return Mockito.spy(new RabbitMqBolt(rabbitMqConfig, new EmptyTupleToRabbitMqMessageConverter()));
    }

    private void prepareMockRabbitMqChannelFactory(RabbitMqBolt rabbitMqBolt) {
        doReturn(rabbitMqChannelFactory).when(rabbitMqBolt).createRabbitMqChannelFactory(any());
    }

    private void prepareSimpleRabbitMqBolt(RabbitMqBolt rabbitMqBolt) {
        rabbitMqBolt.prepare(Collections.emptyMap(), null, mockOutputCollector);
    }
}
