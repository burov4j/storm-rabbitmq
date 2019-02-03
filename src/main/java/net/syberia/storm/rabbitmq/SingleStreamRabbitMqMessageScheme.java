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
import java.util.List;
import java.util.Map;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 *
 * @author Andrey Burov
 */
public abstract class SingleStreamRabbitMqMessageScheme implements RabbitMqMessageScheme {

    private final String streamId;

    public SingleStreamRabbitMqMessageScheme() {
        this(Utils.DEFAULT_STREAM_ID);
    }
    
    public SingleStreamRabbitMqMessageScheme(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public final StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws ConvertionException {
        List<Object> tuple = convertToTuple(envelope, properties, body);
        if (tuple == null || tuple.isEmpty()) {
            return null;
        } else {
            return new StreamedTuple(streamId, tuple);
        }
    }

    public abstract List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws ConvertionException;

    @Override
    public final Map<String, Fields> getStreamsOutputFields() {
        Fields outputFields = getOutputFields();
        return Collections.singletonMap(streamId, outputFields);
    }

    public abstract Fields getOutputFields();
}
