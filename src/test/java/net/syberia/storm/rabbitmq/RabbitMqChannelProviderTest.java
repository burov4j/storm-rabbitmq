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

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqChannelProviderTest extends RabbitMqTest {

    @Test
    public void createConnectionFactory() {
        int requestedHeartbeat = 121,
                port = 8889;
        String host = "testHost",
                password = "testPassword",
                username = "testUsername",
                virtualHost = "testVirtualHost";
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setRequestedHeartbeat(requestedHeartbeat)
                .setHost(host)
                .setPassword(password)
                .setPort(port)
                .setUsername(username)
                .setVirtualHost(virtualHost)
                .build();
        RabbitMqChannelProvider rabbitMqChannelProvider = new RabbitMqChannelProvider(rabbitMqConfig);
        ConnectionFactory connectionFactory = rabbitMqChannelProvider.createConnectionFactory();
        assertEquals(requestedHeartbeat, connectionFactory.getRequestedHeartbeat());
        assertEquals(port, connectionFactory.getPort());
        assertEquals(host, connectionFactory.getHost());
        assertEquals(password, connectionFactory.getPassword());
        assertEquals(username, connectionFactory.getUsername());
        assertEquals(virtualHost, connectionFactory.getVirtualHost());
    }
    
    @Test
    public void prepareWithAddresses() throws IOException, TimeoutException {
        String addresses = "10.189.21.119:8080,10.189.21.118:8181";
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setAddresses(addresses)
                .build();
        RabbitMqChannelProvider rabbitMqChannelProvider = spy(new RabbitMqChannelProvider(rabbitMqConfig));
        doReturn(mockConnectionFactory).when(rabbitMqChannelProvider).createConnectionFactory();
        rabbitMqChannelProvider.prepare();
        verify(mockConnectionFactory, times(1)).newConnection(Address.parseAddresses(addresses));
    }

    @Test
    public void cleanupWithoutPrepare() throws Exception {
        RabbitMqChannelProvider rabbitMqChannelProvider = new RabbitMqChannelProvider();
        rabbitMqChannelProvider.cleanup();
    }

    @Test
    public void prepareAndUse() throws Exception {
        RabbitMqChannelProvider rabbitMqChannelProvider = spy(RabbitMqChannelProvider.class);
        doReturn(mockConnectionFactory).when(rabbitMqChannelProvider).createConnectionFactory();
        rabbitMqChannelProvider.prepare();
        Channel channel = rabbitMqChannelProvider.getChannel();
        rabbitMqChannelProvider.returnChannel(channel);
        rabbitMqChannelProvider.cleanup();
    }
    
    @Test
    public void equals() throws Exception {
        RabbitMqChannelProvider provider1 = new RabbitMqChannelProvider(),
                provider2 = new RabbitMqChannelProvider();
        assertEquals(provider1, provider2);
    }
    
    @Test
    public void notEquals() {
        Map<String, Object> rabbitMqConf = new HashMap<>(1);
        rabbitMqConf.put(RabbitMqConfig.KEY_HOST, "anotherHost");
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfig(rabbitMqConf);
        RabbitMqChannelProvider provider1 = new RabbitMqChannelProvider(),
                provider2 = new RabbitMqChannelProvider(rabbitMqConfig);
        assertNotEquals(provider1, provider2);
    }
    
    @Test
    public void hashCodeEquals() throws Exception {
        RabbitMqChannelProvider provider1 = new RabbitMqChannelProvider(),
                provider2 = new RabbitMqChannelProvider();
        assertEquals(provider1.hashCode(), provider2.hashCode());
    }
    
    @Test
    public void hashCodeNotEquals() {
        Map<String, Object> rabbitMqConf = new HashMap<>(1);
        rabbitMqConf.put(RabbitMqConfig.KEY_HOST, "anotherHost");
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfig(rabbitMqConf);
        RabbitMqChannelProvider provider1 = new RabbitMqChannelProvider(),
                provider2 = new RabbitMqChannelProvider(rabbitMqConfig);
        assertNotEquals(provider1.hashCode(), provider2.hashCode());
    }

}
