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

/**
 *
 * @author Andrey Burov
 */
@SuppressWarnings("WeakerAccess")
public class RabbitMqConfigBuilder {

    private RabbitMqConfig rabbitMqConfig = new RabbitMqConfig();

    public RabbitMqConfigBuilder setHost(String host) {
        rabbitMqConfig.setHost(host);
        return this;
    }

    public RabbitMqConfigBuilder setPort(int port) {
        rabbitMqConfig.setPort(port);
        return this;
    }
    
    public RabbitMqConfigBuilder setAddresses(String addresses) {
        rabbitMqConfig.setAddresses(addresses);
        return this;
    }

    public RabbitMqConfigBuilder setUsername(String username) {
        rabbitMqConfig.setUsername(username);
        return this;
    }

    public RabbitMqConfigBuilder setPassword(String password) {
        rabbitMqConfig.setPassword(password);
        return this;
    }
    
    public RabbitMqConfigBuilder setVirtualHost(String virtualHost) {
        rabbitMqConfig.setVirtualHost(virtualHost);
        return this;
    }
    
    public RabbitMqConfigBuilder setRequestedHeartbeat(int requestedHeartbeat) {
        rabbitMqConfig.setRequestedHeartbeat(requestedHeartbeat);
        return this;
    }

    public RabbitMqConfig build() {
        RabbitMqConfig resultRabbitMqConfig = rabbitMqConfig;
        rabbitMqConfig = new RabbitMqConfig();
        return resultRabbitMqConfig;
    }
}
