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

import java.util.HashMap;
import java.util.Map;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqConfigTest {

    @Test
    public void confMapConstructor() {
        int requestedHeartbeat = 121,
                port = 8889;
        String host = "testHost",
                password = "testPassword",
                username = "testUsername",
                virtualHost = "testVirtualHost";
        Map<String, Object> rabbitMqConf = new HashMap<>(6);
        rabbitMqConf.put(RabbitMqConfig.KEY_REQUESTED_HEARTBEAT, requestedHeartbeat);
        rabbitMqConf.put(RabbitMqConfig.KEY_PORT, port);
        rabbitMqConf.put(RabbitMqConfig.KEY_HOST, host);
        rabbitMqConf.put(RabbitMqConfig.KEY_PASSWORD, password);
        rabbitMqConf.put(RabbitMqConfig.KEY_USERNAME, username);
        rabbitMqConf.put(RabbitMqConfig.KEY_VIRTUAL_HOST, virtualHost);
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfig(rabbitMqConf);
        assertEquals(requestedHeartbeat, rabbitMqConfig.getRequestedHeartbeat());
        assertEquals(port, rabbitMqConfig.getPort());
        assertEquals(host, rabbitMqConfig.getHost());
        assertEquals(password, rabbitMqConfig.getPassword());
        assertEquals(username, rabbitMqConfig.getUsername());
        assertEquals(virtualHost, rabbitMqConfig.getVirtualHost());
    }
    
    @Test
    public void equals() {
        RabbitMqConfig rabbitMqConfig1 = new RabbitMqConfig(),
                rabbitMqConfig2 = new RabbitMqConfig();
        assertEquals(rabbitMqConfig1, rabbitMqConfig2);
    }
    
    @Test
    public void notEquals() {
        Map<String, Object> rabbitMqConf = new HashMap<>(1);
        rabbitMqConf.put(RabbitMqConfig.KEY_HOST, "anotherHost");
        RabbitMqConfig rabbitMqConfig1 = new RabbitMqConfig(),
                rabbitMqConfig2 = new RabbitMqConfig(rabbitMqConf);
        assertNotEquals(rabbitMqConfig1, rabbitMqConfig2);
    }
    
    @Test
    public void hashCodeEquals() {
        RabbitMqConfig rabbitMqConfig1 = new RabbitMqConfig(),
                rabbitMqConfig2 = new RabbitMqConfig();
        assertEquals(rabbitMqConfig1.hashCode(), rabbitMqConfig2.hashCode());
    }
    
    @Test
    public void hashCodeNotEquals() {
        Map<String, Object> rabbitMqConf = new HashMap<>(1);
        rabbitMqConf.put(RabbitMqConfig.KEY_HOST, "anotherHost");
        RabbitMqConfig rabbitMqConfig1 = new RabbitMqConfig(),
                rabbitMqConfig2 = new RabbitMqConfig(rabbitMqConf);
        assertNotEquals(rabbitMqConfig1.hashCode(), rabbitMqConfig2.hashCode());
    }

}
