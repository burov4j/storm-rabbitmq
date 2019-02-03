package net.syberia.storm.rabbitmq.it;

import com.rabbitmq.client.*;
import net.syberia.storm.rabbitmq.RabbitMqBolt;
import net.syberia.storm.rabbitmq.RabbitMqConfig;
import net.syberia.storm.rabbitmq.RabbitMqConfigBuilder;
import net.syberia.storm.rabbitmq.RabbitMqSpout;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Andrey Burov
 */
public class StormRabbitMqIT {

    private static final String QUEUE_INPUT = "input";
    private static final String QUEUE_OUTPUT = "output";

    @Rule
    public GenericContainer rabbitMq = new GenericContainer("rabbitmq:3.7.10")
            .withExposedPorts(ConnectionFactory.DEFAULT_AMQP_PORT);

    private Connection rabbitMqConnection;
    private Channel rabbitMqChannel;
    private LocalCluster localCluster;

    @Before
    public void setUp() throws Exception {
        setUpRabbitMq();
        setUpStorm();
    }

    private void setUpRabbitMq() throws Exception {
        ConnectionFactory rabbitMqConnectionFactory = new ConnectionFactory();
        rabbitMqConnectionFactory.setHost(rabbitMq.getContainerIpAddress());
        rabbitMqConnectionFactory.setPort(rabbitMq.getFirstMappedPort());
        rabbitMqConnection = rabbitMqConnectionFactory.newConnection();
        rabbitMqChannel = rabbitMqConnection.createChannel();
        rabbitMqChannel.queueDeclare(QUEUE_INPUT, false, false, false, Collections.emptyMap());
        rabbitMqChannel.queueDeclare(QUEUE_OUTPUT, false, false, false, Collections.emptyMap());
    }

    private void setUpStorm() {
        localCluster = new LocalCluster();
        RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setHost(rabbitMq.getContainerIpAddress())
                .setPort(rabbitMq.getFirstMappedPort())
                .build();
        TopologyBuilder builder = new TopologyBuilder();
        MessageScheme messageScheme = new MessageScheme();
        builder.setSpout("rabbitmq-spout", new RabbitMqSpout(rabbitMqConfig, messageScheme))
                .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, QUEUE_INPUT);
        MessageConverter messageConverter = new MessageConverter();
        builder.setBolt("rabbitmq-bolt", new RabbitMqBolt(rabbitMqConfig, messageConverter), 1)
                .shuffleGrouping("rabbitmq-spout");
        StormTopology stormTopology = builder.createTopology();
        localCluster.submitTopology("testTopology", Collections.emptyMap(), stormTopology);
    }

    @Test
    public void basicUsage() throws Exception {
        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        String expectedMessage = "test message";
        rabbitMqChannel.basicPublish("", QUEUE_INPUT, properties,
                expectedMessage.getBytes(StandardCharsets.UTF_8));
        MessageHolder actualMessageHolder = new MessageHolder();
        CountDownLatch latch = new CountDownLatch(1);
        rabbitMqChannel.basicConsume(QUEUE_OUTPUT, true, new DefaultConsumer(rabbitMqChannel) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) {
                actualMessageHolder.message = new String(body, StandardCharsets.UTF_8);
                latch.countDown();
            }
        });
        boolean countReachedZero = latch.await(60, TimeUnit.SECONDS);
        if (countReachedZero) {
            assertEquals("Incorrect message received from output queue",
                    expectedMessage, actualMessageHolder.message);
        } else {
            fail("No message received from output queue");
        }
    }

    private class MessageHolder {
        String message;
    }

    @After
    public void tearDown() throws Exception {
        rabbitMqConnection.close();
        localCluster.shutdown();
    }
}
