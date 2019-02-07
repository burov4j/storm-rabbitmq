# Storm RabbitMQ

[![Build Status](https://travis-ci.org/burov4j/storm-rabbitmq.svg)](https://travis-ci.org/burov4j/storm-rabbitmq)
[![codecov](https://codecov.io/gh/burov4j/storm-rabbitmq/branch/master/graph/badge.svg)](https://codecov.io/gh/burov4j/storm-rabbitmq)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/ru.burov4j.storm/storm-rabbitmq/badge.svg)](http://search.maven.org/#search|gav|1|g:"ru.burov4j.storm"%20AND%20a:"storm-rabbitmq")

Provides implementations of [IRichSpout](https://storm.apache.org/releases/1.2.2/javadocs/org/apache/storm/topology/IRichSpout.html) and [IRichBolt](https://storm.apache.org/releases/1.2.2/javadocs/org/apache/storm/topology/IRichBolt.html) for [RabbitMQ](https://www.rabbitmq.com/).

## RabbitMQ Connection

You can set RabbitMQ connection properties using RabbitMqConfigBuilder:

```java
RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setAddresses("localhost:5672")
                .setUsername("guest")
                .setPassword("guest")
                .setRequestedHeartbeat(60)
                .setVirtualHost("/")
                .build();

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("rabbitmq-spout", new RabbitMqSpout(rabbitMqConfig, scheme))
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue");
```

The same with Storm's API:

```java
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("rabbitmq-spout", new RabbitMqSpout(scheme))
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue")
       .addConfiguration(RabbitMqConfig.KEY_ADDRESSES, "localhost:5672")
       .addConfiguration(RabbitMqConfig.KEY_USERNAME, "guest")
       .addConfiguration(RabbitMqConfig.KEY_PASSWORD, "guest")
       .addConfiguration(RabbitMqConfig.KEY_REQUESTED_HEARTBEAT, 60)
       .addConfiguration(RabbitMqConfig.KEY_VIRTUAL_HOST, "/");
```

It is not required to set all of properties: for example, you can set only RabbitMQ address.
In the case another properties will set as defaults:

```java
RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setAddresses("localhost:5672")
                .build();

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("rabbitmq-spout", new RabbitMqSpout(rabbitMqConfig, scheme))
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue");
```

## RabbitMQ Spout

RabbitMqSpout deserializes input messages and then sends it in your Storm's topology.
For using the class you should implement RabbitMqMessageScheme interface:

```java
class MyRabbitMqMessageScheme implements RabbitMqMessageScheme {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }

    @Override
    public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws ConvertionException {
        // your implementation here
    }

    @Override
    public Map<String, Fields> getStreamsOutputFields() {
        // your implementation here
    }

    @Override
    public void cleanup() {
        // your implementation here
    }
}
```

If you want to use only one output stream you can extends SingleStreamRabbitMqMessageScheme:

```java
class MyRabbitMqMessageScheme extends SingleStreamRabbitMqMessageScheme {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }
                
    @Override
    public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws ConvertionException {
        // your implementation here
    }
    
    @Override
    public Fields getOutputFields() {
        // your implementation here
    }
    
    @Override
    public void cleanup() {
        // your implementation here
    }
}
```

The next step is to pass your custom scheme to RabbitMqSpout:

```java
MyRabbitMqMessageScheme scheme = new MyRabbitMqMessageScheme();
RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(scheme);
```

You can also set some properties for RabbitMqSpout:

```java
builder.setSpout("rabbitmq-spout", rabbitMqSpout)
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue") // required
       .addConfiguration(RabbitMqSpout.KEY_AUTO_ACK, false)
       .addConfiguration(RabbitMqSpout.KEY_PREFETCH_COUNT, 64)
       .addConfiguration(RabbitMqSpout.KEY_REQUEUE_ON_FAIL, false);
```

Note that the property RabbitMqSpout.KEY_QUEUE_NAME is required.

## RabbitMQ Bolt

If you want to send messages from your Storm's topology to RabbitMQ, you can use RabbitMqBolt.
In the case you should implement TupleToRabbitMqMessageConverter interface:

```java
class MyTupleToRabbitMqMessageConverter implements TupleToRabbitMqMessageConverter {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }

    @Override
    public String getExchange(Tuple tuple) throws ConvertionException {
        // your implementation here
    }

    @Override
    public String getRoutingKey(Tuple tuple) throws ConvertionException {
        // your implementation here
    }

    @Override
    public AMQP.BasicProperties getProperties(Tuple tuple) throws ConvertionException {
        // your implementation here
    }

    @Override
    public byte[] getMessageBody(Tuple tuple) throws ConvertionException {
        // your implementation here
    }

    @Override
    public void cleanup() {
        // your implementation here
    }
}
```

The next step is to pass your custom converter to RabbitMqBolt:

```java
MyTupleToRabbitMqMessageConverter converter = new MyTupleToRabbitMqMessageConverter();
RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(converter);
```

You can also set some properties for RabbitMqBolt:

```java
builder.setBolt("rabbitmq-bolt", rabbitMqBolt)
       .addConfiguration(RabbitMqBolt.KEY_MANDATORY, false)
       .addConfiguration(RabbitMqBolt.KEY_IMMEDIATE, false);
```

You can read more information about RabbitMQ properties here: https://www.rabbitmq.com/amqp-0-9-1-reference.html
