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

import static junit.framework.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqConfigBuilderTest {

    @Test
    public void basicUsage() {
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
        assertEquals(requestedHeartbeat, rabbitMqConfig.getRequestedHeartbeat());
        assertEquals(port, rabbitMqConfig.getPort());
        assertEquals(host, rabbitMqConfig.getHost());
        assertEquals(password, rabbitMqConfig.getPassword());
        assertEquals(username, rabbitMqConfig.getUsername());
        assertEquals(virtualHost, rabbitMqConfig.getVirtualHost());
    }

}
