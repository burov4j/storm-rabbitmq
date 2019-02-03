package ru.burov4j.storm.rabbitmq.it;

import com.rabbitmq.client.AMQP;
import ru.burov4j.storm.rabbitmq.TupleToRabbitMqMessageConverter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author Andrey Burov
 */
class MessageConverter implements TupleToRabbitMqMessageConverter {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // no operation
    }

    @Override
    public String getExchange(Tuple tuple) {
        return "";
    }

    @Override
    public String getRoutingKey(Tuple tuple) {
        return "output";
    }

    @Override
    public AMQP.BasicProperties getProperties(Tuple tuple) {
        return new AMQP.BasicProperties();
    }

    @Override
    public byte[] getMessageBody(Tuple tuple) {
        return tuple.getBinaryByField("body");
    }

    @Override
    public void cleanup() {
        // no operation
    }
}
