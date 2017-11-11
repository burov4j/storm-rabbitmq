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
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Andrey Burov
 */
public class AutorecoverableQueueingConsumerTest {
    
    @Test
    public void basicUsage() throws IOException, InterruptedException {
        AutorecoverableQueueingConsumer consumer = new AutorecoverableQueueingConsumer(null);
        Envelope envelope = new Envelope(0, true, null, null);
        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        byte[] messageBody = "messageBody".getBytes();
        consumer.handleDelivery(null, envelope, properties, messageBody);
        RabbitMqMessage message1 = consumer.nextMessage(100);
        assertEquals(envelope, message1.getEnvelope());
        assertEquals(properties, message1.getProperties());
        assertArrayEquals(messageBody, message1.getBody());
        RabbitMqMessage message2 = consumer.nextMessage(100);
        assertNull(message2);
    }
    
}
