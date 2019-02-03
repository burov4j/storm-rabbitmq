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

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 *
 * @author Andrey Burov
 */
public class RabbitMqSpoutTest extends StormRabbitMqTest {

    private static final long TEST_MESSAGE_ID = 42;

    private static Map<String, Object> MINIMUM_CONF = Collections.singletonMap(RabbitMqSpout.KEY_QUEUE_NAME, "testQueue");

    private SpoutOutputCollector mockSpoutOutputCollector;
    private TopologyContext mockTopologyContext;

    @Before
    public void setUp() {
        mockSpoutOutputCollector = mock(SpoutOutputCollector.class);
        mockTopologyContext = mock(TopologyContext.class);
        doReturn("my-test-spout").when(mockTopologyContext).getThisComponentId();
    }

    @Test(expected = IllegalArgumentException.class)
    public void queueNotSpecified() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        rabbitMqSpout.open(Collections.EMPTY_MAP, mockTopologyContext, mockSpoutOutputCollector);
    }

    @Test
    public void messageFiltered() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.activate();

        prepareMessageForConsumer(rabbitMqSpout);

        rabbitMqSpout.nextTuple();
        rabbitMqSpout.close();

        verify(mockChannel, times(1)).basicAck(TEST_MESSAGE_ID, false);
        verify(rabbitMqChannelFactory, times(1)).cleanup();
    }

    @Test
    public void messageFilteredAndUnableToAck() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.activate();

        prepareMessageForConsumer(rabbitMqSpout);

        doThrow(IOException.class).when(mockChannel).basicAck(TEST_MESSAGE_ID, false);

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(1)).basicAck(TEST_MESSAGE_ID, false);
        verify(mockSpoutOutputCollector, times(1)).reportError(any(IOException.class));
    }

    @Test
    public void messageEmitted() throws InterruptedException, IOException {
        String streamId = "testStream";
        List<Object> tuple = Collections.singletonList("testValue");
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                return new StreamedTuple(streamId, tuple);
            }
        }));
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        openSimpleRabbitMqSpout(rabbitMqSpout);
        rabbitMqSpout.activate();

        prepareMessageForConsumer(rabbitMqSpout);

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(0)).basicAck(TEST_MESSAGE_ID, false);
        verify(mockSpoutOutputCollector, times(1)).emit(streamId, tuple, TEST_MESSAGE_ID);
    }

    @Test
    public void unableToConvertRabbitMqMessage() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                throw new RuntimeException();
            }
        }));
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        openSimpleRabbitMqSpout(rabbitMqSpout);
        rabbitMqSpout.activate();

        prepareMessageForConsumer(rabbitMqSpout);

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(1)).basicReject(TEST_MESSAGE_ID, false);
        verify(mockSpoutOutputCollector, times(1)).reportError(any(RuntimeException.class));
    }

    @Test
    public void unableToConvertRabbitMqMessageAndUnableToReject() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme() {
            @Override
            public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                throw new RuntimeException();
            }
        }));
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        openSimpleRabbitMqSpout(rabbitMqSpout);
        rabbitMqSpout.activate();

        prepareMessageForConsumer(rabbitMqSpout);

        doThrow(IOException.class).when(mockChannel).basicReject(TEST_MESSAGE_ID, false);

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(1)).basicReject(TEST_MESSAGE_ID, false);
        verify(mockSpoutOutputCollector, times(2)).reportError(any(Exception.class));
    }

    private void prepareMessageForConsumer(RabbitMqSpout rabbitMqSpout) throws InterruptedException {
        Envelope envelope = new Envelope(TEST_MESSAGE_ID, false, null, null);
        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        RabbitMqMessage message = new RabbitMqMessage(envelope, null, null);
        when(mockQueueingConsumer.nextMessage(1L)).thenReturn(message, new RabbitMqMessage[]{null});
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;
    }

    @Test
    public void nextMessageInterrupted() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.activate();

        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        when(mockQueueingConsumer.nextMessage(1L)).thenThrow(InterruptedException.class);
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(0)).basicAck(anyLong(), anyBoolean());
        verify(mockChannel, times(0)).basicReject(anyLong(), anyBoolean());
        verify(mockSpoutOutputCollector, times(0)).emit(anyString(), any(), anyLong());
    }

    @Test
    public void nextMessageNull() throws InterruptedException, IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.activate();

        AutorecoverableQueueingConsumer mockQueueingConsumer = mock(AutorecoverableQueueingConsumer.class);
        when(mockQueueingConsumer.nextMessage(1L)).thenReturn(null);
        rabbitMqSpout.queueingConsumer = mockQueueingConsumer;

        rabbitMqSpout.nextTuple();

        verify(mockChannel, times(0)).basicAck(anyLong(), anyBoolean());
        verify(mockChannel, times(0)).basicReject(anyLong(), anyBoolean());
        verify(mockSpoutOutputCollector, times(0)).emit(anyString(), any(), anyLong());
    }

    @Test
    public void messageAck() throws IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.ack(TEST_MESSAGE_ID);

        verify(mockChannel, times(1)).basicAck(TEST_MESSAGE_ID, false);
    }

    @Test
    public void messageFail() throws IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.fail(TEST_MESSAGE_ID);

        verify(mockChannel, times(1)).basicReject(TEST_MESSAGE_ID, false);
    }

    @Test
    public void processMessageFailed() throws IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        doThrow(IOException.class).when(mockChannel).basicAck(TEST_MESSAGE_ID, false);
        rabbitMqSpout.ack(TEST_MESSAGE_ID);

        verify(mockSpoutOutputCollector, times(1)).reportError(any(IOException.class));
    }

    @Test
    public void processMessageAutoack() throws IOException {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        Map<String, Object> conf = new HashMap<>(MINIMUM_CONF);
        conf.put(RabbitMqSpout.KEY_AUTO_ACK, true);
        rabbitMqSpout.open(conf, mockTopologyContext, mockSpoutOutputCollector);

        doThrow(IOException.class).when(mockChannel).basicAck(TEST_MESSAGE_ID, false);

        rabbitMqSpout.ack(TEST_MESSAGE_ID);

        verify(mockSpoutOutputCollector, times(0)).reportError(any(IOException.class));
    }

    @Test
    public void alreadyClosedExceptionAck() throws IOException {
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        doThrow(AlreadyClosedException.class).when(mockChannel).basicAck(TEST_MESSAGE_ID, false);
        rabbitMqSpout.ack(TEST_MESSAGE_ID);

        verify(mockSpoutOutputCollector, times(0)).reportError(any(IOException.class));
    }

    @Test
    public void initializerUsage() throws IOException {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        rabbitMqSpout.setInitializer(Channel::queueDeclare);
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        openSimpleRabbitMqSpout(rabbitMqSpout);

        verify(mockChannel, times(1)).queueDeclare();
    }

    @Test(expected = RuntimeException.class)
    public void initializerException() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        rabbitMqSpout.setInitializer((Channel channel) -> {
            throw new IOException();
        });
        openSimpleRabbitMqSpout(rabbitMqSpout);
    }

    @Test(expected = RuntimeException.class)
    public void unableToPrepare() throws IOException, TimeoutException {
        doThrow(IOException.class).when(rabbitMqChannelFactory).prepare();
        createAndOpenSimpleRabbitMqSpout();
    }

    @Test(expected = RuntimeException.class)
    public void unableToCreateChannel() throws IOException {
        doThrow(IOException.class).when(rabbitMqChannelFactory).createChannel();
        createAndOpenSimpleRabbitMqSpout();
    }

    @Test(expected = RuntimeException.class)
    public void unableToSetQos() throws IOException {
        doThrow(IOException.class).when(mockChannel).basicQos(anyInt());
        createAndOpenSimpleRabbitMqSpout();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidPrefetchCount() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        Map<String, Object> conf = new HashMap<>(MINIMUM_CONF);
        conf.put(RabbitMqSpout.KEY_PREFETCH_COUNT, Integer.MIN_VALUE);
        rabbitMqSpout.open(conf, mockTopologyContext, mockSpoutOutputCollector);
    }

    @Test(expected = RuntimeException.class)
    public void unableToSetConsume() throws IOException {
        doThrow(IOException.class).when(mockChannel).basicConsume(anyString(), anyBoolean(), anyString(), any());
        createAndOpenSimpleRabbitMqSpout();
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
    public void unableToCloseIOException() throws IOException {
        doThrow(IOException.class).when(rabbitMqChannelFactory).cleanup();
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.close();
    }

    @Test
    public void unableToCloseAlreadyClosedException() throws IOException {
        doThrow(AlreadyClosedException.class).when(rabbitMqChannelFactory).cleanup();
        RabbitMqSpout rabbitMqSpout = createAndOpenSimpleRabbitMqSpout();
        rabbitMqSpout.close();
    }

    @Test
    public void notActiveNextTuple() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        rabbitMqSpout.deactivate();
        rabbitMqSpout.nextTuple();
    }

    @Test
    public void createRabbitMqChannelFactoryStormConf() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        Map<String, Object> conf = Collections.singletonMap(RabbitMqConfig.KEY_USERNAME, "user1");
        RabbitMqChannelFactory channelFactory = rabbitMqSpout.createRabbitMqChannelFactory(conf);
        ConnectionFactory connectionFactory = channelFactory.createConnectionFactory();

        assertEquals("user1", connectionFactory.getUsername());
    }

    @Test
    public void createRabbitMqChannelFactoryRabbitMqConf() {
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setUsername("user1")
                .build();
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout(rabbitMqConfig);
        Map<String, Object> conf = Collections.singletonMap(RabbitMqConfig.KEY_USERNAME, "user2");
        RabbitMqChannelFactory channelFactory = rabbitMqSpout.createRabbitMqChannelFactory(conf);
        ConnectionFactory connectionFactory = channelFactory.createConnectionFactory();

        assertEquals("user1", connectionFactory.getUsername());
    }

    private RabbitMqSpout createAndOpenSimpleRabbitMqSpout() {
        RabbitMqSpout rabbitMqSpout = createSimpleRabbitMqSpout();
        prepareMockRabbitMqChannelFactory(rabbitMqSpout);
        openSimpleRabbitMqSpout(rabbitMqSpout);
        return rabbitMqSpout;
    }

    private RabbitMqSpout createSimpleRabbitMqSpout() {
        return spy(new RabbitMqSpout(new EmptyRabbitMqMessageScheme()));
    }

    private RabbitMqSpout createSimpleRabbitMqSpout(RabbitMqConfig rabbitMqConfig) {
        return spy(new RabbitMqSpout(rabbitMqConfig, new EmptyRabbitMqMessageScheme()));
    }

    private void prepareMockRabbitMqChannelFactory(RabbitMqSpout rabbitMqSpout) {
        doReturn(rabbitMqChannelFactory).when(rabbitMqSpout).createRabbitMqChannelFactory(any());
    }

    private void openSimpleRabbitMqSpout(RabbitMqSpout rabbitMqSpout) {
        rabbitMqSpout.open(MINIMUM_CONF, mockTopologyContext, mockSpoutOutputCollector);
    }
}
