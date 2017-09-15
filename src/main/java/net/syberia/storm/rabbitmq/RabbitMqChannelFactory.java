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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 *
 * @author Andrey Burov
 */
class RabbitMqChannelFactory extends BasePooledObjectFactory<Channel> implements AutoCloseable {

    private final Connection rabbitMqConnection;

    public RabbitMqChannelFactory(ConnectionFactory rabbitMqConnectionFactory) throws IOException, TimeoutException {
        this.rabbitMqConnection = rabbitMqConnectionFactory.newConnection();
    }
    
    public RabbitMqChannelFactory(ConnectionFactory rabbitMqConnectionFactory, Address[] addresses) throws IOException, TimeoutException {
        this.rabbitMqConnection = rabbitMqConnectionFactory.newConnection(addresses);
    }

    @Override
    public Channel create() throws Exception {
        return this.rabbitMqConnection.createChannel();
    }

    @Override
    public PooledObject<Channel> wrap(Channel rabbitMqChannel) {
        return new DefaultPooledObject<>(rabbitMqChannel);
    }

    @Override
    public void destroyObject(PooledObject<Channel> pooledChannel) throws Exception {
        pooledChannel.getObject().close();
    }

    @Override
    public void close() throws Exception {
        this.rabbitMqConnection.close();
    }

}
