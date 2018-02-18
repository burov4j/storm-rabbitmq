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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.fail;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
        byte[] messageBody = "testMessageBody".getBytes("UTF8");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().build();
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter() {
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
        });
        rabbitMqBolt.declareOutputFields(null);
        Map<String, Object> stormConf = new HashMap<>(2);
        stormConf.put(RabbitMqBolt.KEY_MANDATORY, true);
        stormConf.put(RabbitMqBolt.KEY_IMMEDIATE, true);
        rabbitMqBolt.prepare(stormConf, null, mockOutputCollector);
        Tuple mockTuple = mock(Tuple.class);
        rabbitMqBolt.execute(mockTuple);
        rabbitMqBolt.cleanup();
        verify(mockChannel, times(1)).basicPublish(exchange, routingKey, true, true, properties, messageBody);
        verify(mockOutputCollector, times(1)).ack(mockTuple);
        verify(rabbitMqChannelProvider, times(1)).cleanup();
    }

    @Test
    public void unableToPrepare() throws IOException, TimeoutException {
        doThrow(IOException.class).when(rabbitMqChannelProvider).prepare();
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter());
        try {
            rabbitMqBolt.prepare(null, null, null);
            fail("RuntimeException expected");
        } catch (RuntimeException ex) {
            // the test passed
        }
    }

    @Test(expected = Exception.class)
    public void unableToGetChannel() throws Exception {
        doThrow(Exception.class).when(rabbitMqChannelProvider).getChannel();
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter());
        rabbitMqBolt.prepare(Collections.emptyMap(), null, mockOutputCollector);
    }

    @Test
    public void unableToPublish() throws Exception {
        doThrow(IOException.class).when(mockChannel).basicPublish(any(), any(), eq(false), eq(false), any(), any());
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter());
        rabbitMqBolt.prepare(Collections.emptyMap(), null, mockOutputCollector);
        Tuple tuple = mock(Tuple.class);
        rabbitMqBolt.execute(tuple);
        verify(mockOutputCollector, times(1)).reportError(any(IOException.class));
        verify(mockOutputCollector, times(1)).fail(tuple);
    }

    @Test
    public void unableToGetMessageBody() throws Exception {
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter() {
            @Override
            public byte[] getMessageBody(Tuple tuple) {
                throw new RuntimeException();
            }
        });
        rabbitMqBolt.prepare(Collections.emptyMap(), null, mockOutputCollector);
        Tuple tuple = mock(Tuple.class);
        rabbitMqBolt.execute(tuple);
        verify(mockOutputCollector, times(1)).reportError(any(RuntimeException.class));
        verify(mockOutputCollector, times(1)).fail(tuple);
    }

    @Test
    public void unableToCleanup() throws Exception {
        doThrow(Exception.class).when(rabbitMqChannelProvider).cleanup();
        RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(rabbitMqChannelProvider, new EmptyTupleToRabbitMqMessageConverter());
        rabbitMqBolt.cleanup();
    }

}
