package ru.burov4j.storm.rabbitmq.it;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import ru.burov4j.storm.rabbitmq.SingleStreamRabbitMqMessageScheme;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Burov
 */
class MessageScheme extends SingleStreamRabbitMqMessageScheme {

    @Override
    public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        return Collections.singletonList(body);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("body");
    }

    @Override
    public void prepare(Map config, TopologyContext context) {
        // no operation
    }

    @Override
    public void cleanup() {
        // no operation
    }
}
