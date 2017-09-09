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
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqChannelPoolTest extends RabbitMqTest {

    private RabbitMqChannelFactory channelFactory;
    private RabbitMqChannelPool channelPool;

    @Before
    public void setUp() throws IOException, TimeoutException {
        channelFactory = new RabbitMqChannelFactory(mockConnectionFactory);
        channelPool = RabbitMqChannelProvider.createRabbitMqChannelPool(channelFactory);
    }

    @Test
    public void unusedPool() throws IOException, TimeoutException {
        verify(mockConnectionFactory, times(1)).newConnection();
        verify(mockConnection, times(0)).createChannel();
    }

    @Test
    public void borrowChannelWithoutReturn() throws Exception {
        int borrowChannelCount = 100;
        for (int i = 0; i < borrowChannelCount; i++) {
            channelPool.borrowObject();
        }
        verify(mockConnectionFactory, times(1)).newConnection();
        verify(mockConnection, times(borrowChannelCount)).createChannel();
    }

    @Test
    public void borrowChannelWithReturn() throws Exception {
        for (int i = 0; i < 100; i++) {
            Channel channel = channelPool.borrowObject();
            channelPool.returnObject(channel);
        }
        verify(mockConnectionFactory, times(1)).newConnection();
        verify(mockConnection, times(1)).createChannel();
    }

    @After
    public void tearDown() throws Exception {
        channelPool.close();
    }

}
