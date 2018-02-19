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
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.fail;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqSpoutTest extends StormRabbitMqTest {

    private static final String TEST_QUEUE_NAME = "testQueue";

    private static Map<String, Object> MINIMUM_CONF;

    private SpoutOutputCollector mockSpoutOutputCollector;
    private TopologyContext mockTopologyContext;

    @BeforeClass
    public static void setUpClass() {
        MINIMUM_CONF = new HashMap<>(1);
        MINIMUM_CONF.put(RabbitMqSpout.KEY_QUEUE_NAME, TEST_QUEUE_NAME);
        MINIMUM_CONF = Collections.unmodifiableMap(MINIMUM_CONF);
    }

    @Before
    public void setUp() {
        mockSpoutOutputCollector = mock(SpoutOutputCollector.class);
        mockTopologyContext = mock(TopologyContext.class);
        doReturn("my-test-spout").when(mockTopologyContext).getThisComponentId();
    }

    @Test(expected = IllegalArgumentException.class)
    public void queueNotSpecified() {
        RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(new EmptyRabbitMqMessageScheme());
        rabbitMqSpout.open(Collections.EMPTY_MAP, mockTopologyContext, mockSpoutOutputCollector);
    }

    @Test
    public void messageFiltered() throws Exception {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        rabbitMqSpout.activate();
        long messageId = 435;
        Envelope envelope = new Envelope(messageId, false, null, null);
        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        RabbitMqMessage message = new RabbitMqMessage(envelope, null, null);
        when(mockQueueingConsumer.nextMessage(anyLong())).thenReturn(message, new RabbitMqMessage[]{null});
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;
        rabbitMqSpout.nextTuple();
        rabbitMqSpout.close();
        verify(mockChannel, times(1)).basicAck(messageId, false);
        verify(rabbitMqChannelProvider, times(1)).cleanup();
    }

    @Test
    public void messageEmitted() throws Exception {
        String streamId = "testStream";
        List<Object> tuple = Collections.singletonList("testValue");
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws Exception {
                return new StreamedTuple(streamId, tuple);
            }
        }));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        rabbitMqSpout.activate();
        long messageId = 435;
        Envelope envelope = new Envelope(messageId, false, null, null);
        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        RabbitMqMessage message = new RabbitMqMessage(envelope, null, null);
        when(mockQueueingConsumer.nextMessage(anyLong())).thenReturn(message, new RabbitMqMessage[]{null});
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;
        rabbitMqSpout.nextTuple();
        verify(mockChannel, times(0)).basicAck(messageId, false);
        verify(mockSpoutOutputCollector, times(1)).emit(streamId, tuple, messageId);
    }

    @Test
    public void unableToConvertRabbitMqMessage() throws Exception {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws Exception {
                throw new RuntimeException();
            }
        }));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        rabbitMqSpout.activate();
        long messageId = 435;
        Envelope envelope = new Envelope(messageId, false, null, null);
        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        RabbitMqMessage message = new RabbitMqMessage(envelope, null, null);
        when(mockQueueingConsumer.nextMessage(anyLong())).thenReturn(message, new RabbitMqMessage[]{null});
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;
        rabbitMqSpout.nextTuple();
        verify(mockChannel, times(1)).basicReject(messageId, false);
        verify(mockSpoutOutputCollector, times(1)).reportError(any(RuntimeException.class));
    }

    @Test
    public void messageAck() throws IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        long messageId = 66453;
        rabbitMqSpout.ack(messageId);
        verify(mockChannel, times(1)).basicAck(messageId, false);
    }

    @Test
    public void messageFail() throws IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, null);
        long messageId = 66453;
        rabbitMqSpout.fail(messageId);
        verify(mockChannel, times(1)).basicReject(messageId, false);
    }

    @Test
    public void processMessageFailed() throws IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        long messageId = 66453;
        doThrow(IOException.class).when(mockChannel).basicAck(messageId, false);
        rabbitMqSpout.ack(messageId);
        verify(mockSpoutOutputCollector, times(1)).reportError(any(IOException.class));
    }

    @Test
    public void initializerUsage() throws IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        rabbitMqSpout.setInitializer(Channel::queueDeclare);
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        verify(mockChannel, times(1)).queueDeclare();
    }

    @Test(expected = RuntimeException.class)
    public void initializerException() {
        RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(new EmptyRabbitMqMessageScheme());
        rabbitMqSpout.setInitializer((Channel channel) -> {
            throw new IOException();
        });
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
    }

    @Test(expected = RuntimeException.class)
    public void unableToOpen() throws IOException, TimeoutException {
        doThrow(IOException.class).when(rabbitMqChannelProvider).prepare();
        RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(new EmptyRabbitMqMessageScheme());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
    }

    @Test
    public void declareOutputFields() {
        Map<String, Fields> outputFields = new HashMap<>(2);
        String streamId1 = "testStream1",
                streamId2 = "testStream2";
        Fields fields1 = new Fields("field1", "field2"),
                fields2 = new Fields("field3", "field4");
        outputFields.put(streamId1, fields1);
        outputFields.put(streamId2, fields2);
        RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public Map<String, Fields> getStreamsOutputFields() {
                return outputFields;
            }
        });
        OutputFieldsDeclarer mockOutputFieldsDeclarer = mock(OutputFieldsDeclarer.class);
        rabbitMqSpout.declareOutputFields(mockOutputFieldsDeclarer);
        verify(mockOutputFieldsDeclarer, times(1)).declareStream(streamId1, fields1);
        verify(mockOutputFieldsDeclarer, times(1)).declareStream(streamId2, fields2);
    }

    @Test
    public void unableToClose() throws Exception {
        doThrow(Exception.class).when(rabbitMqChannelProvider).cleanup();
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
        doReturn(rabbitMqChannelProvider).when(rabbitMqSpout).createRabbitMqChannelProvider(any());
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
        rabbitMqSpout.close();
    }

}
