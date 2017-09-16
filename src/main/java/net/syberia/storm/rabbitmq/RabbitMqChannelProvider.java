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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqChannelProvider implements Serializable {

    private static final long serialVersionUID = 8824907115492553548L;
    
    private static final Set<RabbitMqChannelProvider> knownProviders = new HashSet<>();

    private final RabbitMqConfig rabbitMqConfig;

    private transient RabbitMqChannelFactory rabbitMqChannelFactory;
    private transient RabbitMqChannelPool rabbitMqChannelPool;

    RabbitMqChannelProvider() {
        this(new RabbitMqConfig());
    }

    public RabbitMqChannelProvider(RabbitMqConfig rabbitMqConfig) {
        this.rabbitMqConfig = rabbitMqConfig;
        registerProviderIfAbsent();
    }

    synchronized void prepare() throws IOException, TimeoutException {
        if (rabbitMqChannelPool == null) {
            ConnectionFactory rabbitMqConnectionFactory = createConnectionFactory();
            if (rabbitMqConfig.hasAddresses()) {
                Address[] addresses = Address.parseAddresses(rabbitMqConfig.getAddresses());
                this.rabbitMqChannelFactory = new RabbitMqChannelFactory(rabbitMqConnectionFactory, addresses);
            } else {
                this.rabbitMqChannelFactory = new RabbitMqChannelFactory(rabbitMqConnectionFactory);
            }
            this.rabbitMqChannelPool = createRabbitMqChannelPool(rabbitMqChannelFactory);
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
        return channelPool;
    }

    Channel getChannel() throws Exception {
        return rabbitMqChannelPool.borrowObject();
    }

    void returnChannel(Channel channel) {
        rabbitMqChannelPool.returnObject(channel);
    }

    void cleanup() throws Exception {
        if (rabbitMqChannelPool != null) {
            rabbitMqChannelPool.close();
            rabbitMqChannelFactory.close();
        }
    }
    
    private void readObject(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException {
        objectInputStream.defaultReadObject();
        registerProviderIfAbsent();
    }
    
    private void registerProviderIfAbsent() {
        if (!knownProviders.contains(this)) {
            knownProviders.add(this);
        }
    }
    
    private Object readResolve() {
        for (RabbitMqChannelProvider provider : knownProviders) {
            if (provider.equals(this)) {
                return provider;
            }
        }
        return this;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.rabbitMqConfig);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RabbitMqChannelProvider other = (RabbitMqChannelProvider) obj;
        return Objects.equals(this.rabbitMqConfig, other.rabbitMqConfig);
    }

}
