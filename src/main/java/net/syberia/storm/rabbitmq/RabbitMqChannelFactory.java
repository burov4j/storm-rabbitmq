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

/**
 *
 * @author Andrey Burov
 */
class RabbitMqChannelFactory implements AutoCloseable {

    private final Connection rabbitMqConnection;

    public RabbitMqChannelFactory(ConnectionFactory rabbitMqConnectionFactory) throws IOException, TimeoutException {
        this.rabbitMqConnection = rabbitMqConnectionFactory.newConnection();
    }
    
    public RabbitMqChannelFactory(ConnectionFactory rabbitMqConnectionFactory, Address[] addresses) throws IOException, TimeoutException {
        this.rabbitMqConnection = rabbitMqConnectionFactory.newConnection(addresses);
    }

    public Channel createChannel() throws Exception {
        return this.rabbitMqConnection.createChannel();
    }

    public boolean isOpen() {
        return this.rabbitMqConnection.isOpen();
    }

    @Override
    public void close() throws Exception {
        this.rabbitMqConnection.close();
    }

}
