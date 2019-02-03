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
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author Andrey Burov
 */
class EmptyTupleToRabbitMqMessageConverter implements TupleToRabbitMqMessageConverter {

    private static final long serialVersionUID = -7111105887908787772L;

    @Override
    public void prepare(Map config, TopologyContext context) {
        // no operation
    }

    @Override
    public String getExchange(Tuple tuple) {
        return null;
    }

    @Override
    public String getRoutingKey(Tuple tuple) {
        return null;
    }

    @Override
    public AMQP.BasicProperties getProperties(Tuple tuple) {
        return null;
    }

    @Override
    public byte[] getMessageBody(Tuple tuple) {
        return new byte[0];
    }

    @Override
    public void cleanup() {
        // no operation
    }
}
