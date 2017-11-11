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
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import lombok.EqualsAndHashCode;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Andrey Burov
 */
@EqualsAndHashCode(of = "rabbitMqConfig")
public class RabbitMqChannelProvider implements Serializable {

    private static final long serialVersionUID = 8824907115492553548L;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqChannelProvider.class);

    private static final Set<RabbitMqChannelProvider> KNOWN_PROVIDERS = new HashSet<>();

    private final RabbitMqConfig rabbitMqConfig;

    private transient RabbitMqChannelFactory rabbitMqChannelFactory;
    private transient RabbitMqChannelPool rabbitMqChannelPool;

    RabbitMqChannelProvider() { // for testing
        this(new RabbitMqConfig());
    }

    static RabbitMqChannelProvider withStormConfig(Map<String, Object> stormConf) {
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfig(stormConf);
        return withRabbitMqConfig(rabbitMqConfig);
    }

    private Object readResolve() {
        return withRabbitMqConfig(rabbitMqConfig);
    }

    public static synchronized RabbitMqChannelProvider withRabbitMqConfig(RabbitMqConfig rabbitMqConfig) {
        RabbitMqChannelProvider providerWithConfig = KNOWN_PROVIDERS.stream()
                .filter(provider -> provider.rabbitMqConfig.equals(rabbitMqConfig))
                .findFirst()
                .orElse(null);
        if (providerWithConfig == null) {
            providerWithConfig = new RabbitMqChannelProvider(rabbitMqConfig);
            KNOWN_PROVIDERS.add(providerWithConfig);
        }
        return providerWithConfig;
    }

    RabbitMqChannelProvider(RabbitMqConfig rabbitMqConfig) { // package-private for testing
        this.rabbitMqConfig = rabbitMqConfig;
    }

    public synchronized void prepare() throws IOException, TimeoutException {
        if (rabbitMqChannelPool == null || rabbitMqChannelPool.isClosed()) {
            LOGGER.info("Creating RabbitMQ channel pool...");
            ConnectionFactory rabbitMqConnectionFactory = createConnectionFactory();
            if (rabbitMqConfig.hasAddresses()) {
                Address[] addresses = Address.parseAddresses(rabbitMqConfig.getAddresses());
                this.rabbitMqChannelFactory = new RabbitMqChannelFactory(rabbitMqConnectionFactory, addresses);
            } else {
                this.rabbitMqChannelFactory = new RabbitMqChannelFactory(rabbitMqConnectionFactory);
            }
            this.rabbitMqChannelPool = createRabbitMqChannelPool(rabbitMqChannelFactory);
            LOGGER.info("RabbitMQ channel pool was created");
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

    static RabbitMqChannelPool createRabbitMqChannelPool(RabbitMqChannelFactory channelFactory) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setJmxNameBase("storm-rabbitmq:name=");
        config.setJmxNamePrefix("ChannelPool");
        RabbitMqChannelPool channelPool = new RabbitMqChannelPool(channelFactory, config);
        channelPool.setMaxTotal(-1);
        channelPool.setMaxIdle(-1);
        return channelPool;
    }

    public Channel getChannel() throws Exception {
        return rabbitMqChannelPool.borrowObject();
    }

    public void returnChannel(Channel channel) {
        rabbitMqChannelPool.returnObject(channel);
    }

    public void cleanup() throws Exception {
        if (rabbitMqChannelPool != null) {
            rabbitMqChannelPool.close();
            rabbitMqChannelFactory.close();
        }
    }

    private void readObject(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
    }

}
