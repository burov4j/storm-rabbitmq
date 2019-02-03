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
import org.junit.Test;
import org.apache.storm.utils.Utils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.apache.storm.task.TopologyContext;

/**
 *
 * @author Andrey Burov
 */
public class SingleStreamRabbitMqMessageSchemeTest {

    @Test
    public void userStreamId() throws Exception {
        String streamId = "streamId",
                stringField = "stringField",
                stringValue = "stringValue";
        RabbitMqMessageScheme rabbitMqMessageScheme = new SingleStreamRabbitMqMessageScheme(streamId) {
            @Override
            public void prepare(Map config, TopologyContext context) {
                // no operation
            }
            
            @Override
            public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                return Collections.singletonList(stringValue);
            }

            @Override
            public Fields getOutputFields() {
                return new Fields(stringField);
            }

            @Override
            public void cleanup() {
                // no operation
            }
        };

        StreamedTuple streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(null, null, null);
        assertNotNull("Streamed tuple is null", streamedTuple);
        assertEquals(streamId, streamedTuple.getStreamId());
        assertEquals(stringValue, streamedTuple.getTuple().get(0));

        Map<String, Fields> outputFields = rabbitMqMessageScheme.getStreamsOutputFields();
        assertEquals(stringField, outputFields.get(streamId).get(0));
    }

    @Test
    public void defaultStreamId() throws Exception {
        RabbitMqMessageScheme rabbitMqMessageScheme = new SingleStreamRabbitMqMessageScheme() {
            @Override
            public void prepare(Map config, TopologyContext context) {
                // no operation
            }
            
            @Override
            public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                return Collections.singletonList("stringValue");
            }

            @Override
            public Fields getOutputFields() {
                return new Fields("stringField");
            }

            @Override
            public void cleanup() {
                // no operation
            }
        };

        StreamedTuple streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(null, null, null);
        assertNotNull("Streamed tuple is null", streamedTuple);
        assertEquals(Utils.DEFAULT_STREAM_ID, streamedTuple.getStreamId());
    }

    @Test
    public void filterMessageNullTuple() throws Exception {
        RabbitMqMessageScheme rabbitMqMessageScheme = new SingleStreamRabbitMqMessageScheme() {
            @Override
            public void prepare(Map config, TopologyContext context) {
                // no operation
            }
            
            @Override
            public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                return null;
            }

            @Override
            public Fields getOutputFields() {
                return new Fields("stringField");
            }

            @Override
            public void cleanup() {
                // no operation
            }
        };
        StreamedTuple streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(null, null, null);
        assertNull(streamedTuple);
    }

    @Test
    public void filterMessageEmptyTuple() throws Exception {
        RabbitMqMessageScheme rabbitMqMessageScheme = new SingleStreamRabbitMqMessageScheme() {
            @Override
            public void prepare(Map config, TopologyContext context) {
                // no operation
            }

            @Override
            public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                return Collections.emptyList();
            }

            @Override
            public Fields getOutputFields() {
                return new Fields("stringField");
            }

            @Override
            public void cleanup() {
                // no operation
            }
        };
        StreamedTuple streamedTuple = rabbitMqMessageScheme.convertToStreamedTuple(null, null, null);
        assertNull(streamedTuple);
    }
}
