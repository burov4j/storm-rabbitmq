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
import java.util.Collections;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author Andrey Burov
 */
class EmptyRabbitMqMessageScheme implements RabbitMqMessageScheme {

    private static final long serialVersionUID = 7487193577014813339L;

    @Override
    public void prepare(Map config, TopologyContext context) {
        // no operation
    }

    @Override
    public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws Exception {
        return null;
    }

    @Override
    public Map<String, Fields> getStreamsOutputFields() {
        return Collections.EMPTY_MAP;
    }

    @Override
    public void cleanup() {
        // no operation
    }

}
