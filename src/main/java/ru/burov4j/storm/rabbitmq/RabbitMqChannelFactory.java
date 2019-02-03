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

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Andrey Burov
 */
@Slf4j
@EqualsAndHashCode(of = "rabbitMqConfig")
class RabbitMqChannelFactory {

    private static final Set<RabbitMqChannelFactory> KNOWN_FACTORIES = new HashSet<>();

    private final RabbitMqConfig rabbitMqConfig;

    private Connection rabbitMqConnection;

    RabbitMqChannelFactory() { // for testing
        this(new RabbitMqConfig());
    }

    static RabbitMqChannelFactory withStormConfig(Map stormConf) {
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfig(stormConf);
        return withRabbitMqConfig(rabbitMqConfig);
    }

    static synchronized RabbitMqChannelFactory withRabbitMqConfig(RabbitMqConfig rabbitMqConfig) {
        RabbitMqChannelFactory factoryWithConfig = KNOWN_FACTORIES.stream()
                .filter(factory -> factory.rabbitMqConfig.equals(rabbitMqConfig))
                .findFirst()
                .orElse(null);
        if (factoryWithConfig == null) {
            factoryWithConfig = new RabbitMqChannelFactory(rabbitMqConfig);
            KNOWN_FACTORIES.add(factoryWithConfig);
        }
        return factoryWithConfig;
    }

    RabbitMqChannelFactory(RabbitMqConfig rabbitMqConfig) { // package-private for testing
        this.rabbitMqConfig = rabbitMqConfig;
    }

    public synchronized void prepare() throws IOException, TimeoutException {
        if (rabbitMqConnection == null || !rabbitMqConnection.isOpen()) {
            log.info("Creating RabbitMQ connection...");
            ConnectionFactory rabbitMqConnectionFactory = createConnectionFactory();
            if (rabbitMqConfig.hasAddresses()) {
                Address[] addresses = Address.parseAddresses(rabbitMqConfig.getAddresses());
                rabbitMqConnection = rabbitMqConnectionFactory.newConnection(addresses);
            } else {
                rabbitMqConnection = rabbitMqConnectionFactory.newConnection();
            }
            log.info("RabbitMQ connection was created");
        }
    }

    ConnectionFactory createConnectionFactory() { // package-private for testing
        ConnectionFactory rabbitMqConnectionFactory = new ConnectionFactory();
        rabbitMqConnectionFactory.setHost(rabbitMqConfig.getHost());
        rabbitMqConnectionFactory.setPort(rabbitMqConfig.getPort());
        rabbitMqConnectionFactory.setUsername(rabbitMqConfig.getUsername());
        rabbitMqConnectionFactory.setPassword(rabbitMqConfig.getPassword());
        rabbitMqConnectionFactory.setVirtualHost(rabbitMqConfig.getVirtualHost());
        rabbitMqConnectionFactory.setRequestedHeartbeat(rabbitMqConfig.getRequestedHeartbeat());
        return rabbitMqConnectionFactory;
    }

    Channel createChannel() throws IOException {
        return rabbitMqConnection.createChannel();
    }

    public void cleanup() throws IOException {
        if (rabbitMqConnection != null) {
            rabbitMqConnection.close();
        }
    }
}
