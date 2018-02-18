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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Andrey Burov
 */
@Slf4j
class AutorecoverableQueueingConsumer extends DefaultConsumer {
    
    private final BlockingQueue<RabbitMqMessage> queue = new LinkedBlockingQueue<>();
    
    public AutorecoverableQueueingConsumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        if (sig.isInitiatedByApplication()) {
            log.info("Handled shutdown signal");
        } else {
            log.error("Handled shutdown signal", sig);
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        RabbitMqMessage rabbitMqMessage = new RabbitMqMessage(envelope, properties, body);
        queue.add(rabbitMqMessage);
    }
    
    public RabbitMqMessage nextMessage(long timeout) throws InterruptedException {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }
    
}
