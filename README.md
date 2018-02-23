# Storm RabbitMQ

[![Build Status](https://travis-ci.org/anderelate/storm-rabbitmq.png)](https://travis-ci.org/anderelate/storm-rabbitmq)
[![codecov](https://codecov.io/gh/anderelate/storm-rabbitmq/branch/master/graph/badge.svg)](https://codecov.io/gh/anderelate/storm-rabbitmq)

Предоставляет базовые реализации интерфейсов [IRichSpout](https://storm.apache.org/releases/1.2.1/javadocs/org/apache/storm/topology/IRichSpout.html) и [IRichBolt](https://storm.apache.org/releases/1.2.1/javadocs/org/apache/storm/topology/IRichBolt.html) для взаимодействия с [RabbitMQ](https://www.rabbitmq.com/) из топологий [Apache Storm](http://storm.apache.org/).

## RabbitMQ Connection

Конфигурацию для подключения к RabbitMQ можно настроить с помощью класса RabbitMqConfigBuilder:

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

Вы можете сделать то же самое через стандартный Storm's API

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

При этом не обязательно указывать все параметры. 
Вы можете задать, например, только адрес RabbitMQ.
В этом случае остальные параметры будут установлены в значения по умолчанию:

```java
RabbitMqConfig rabbitMqConfig = new RabbitMqConfigBuilder()
                .setAddresses("localhost:5672")
                .build();

TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("rabbitmq-spout", new RabbitMqSpout(rabbitMqConfig, scheme))
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue");
```

## RabbitMQ Spout

Класс RabbitMqSpout десериализует входящие сообщения и отправляет их в топологию.
Для его использования вам необходимо реализовать интерфейс RabbitMqMessageScheme:

```java
class MyRabbitMqMessageScheme implements RabbitMqMessageScheme {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }

    @Override
    public StreamedTuple convertToStreamedTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws Exception {
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

Если вам нужен только один исходящий поток, то вы можете использовать класс SingleStreamRabbitMqMessageScheme:

```java
class MyRabbitMqMessageScheme extends SingleStreamRabbitMqMessageScheme {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }
                
    @Override
    public List<Object> convertToTuple(Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws Exception {
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

Далее вам необходимо передать вашу реализацию в RabbitMqSpout:

```java
MyRabbitMqMessageScheme scheme = new MyRabbitMqMessageScheme();
RabbitMqSpout rabbitMqSpout = new RabbitMqSpout(scheme);
```

Для RabbitMqSpout также можно указать различные параметры:

```java
builder.setSpout("rabbitmq-spout", rabbitMqSpout)
       .addConfiguration(RabbitMqSpout.KEY_QUEUE_NAME, "myQueue") // required
       .addConfiguration(RabbitMqSpout.KEY_AUTO_ACK, false)
       .addConfiguration(RabbitMqSpout.KEY_PREFETCH_COUNT, 64)
       .addConfiguration(RabbitMqSpout.KEY_REQUEUE_ON_FAIL, false);
```

При этом параметр RabbitMqSpout.KEY_QUEUE_NAME является обязательным.

## RabbitMQ Bolt

Если вы хотите отправлять сообщения из топологии в RabbitMQ, то вы можете использовать класс RabbitMqBolt.
Для его использования вам необходимо реализовать интерфейс TupleToRabbitMqMessageConverter:

```java
class MyTupleToRabbitMqMessageConverter implements TupleToRabbitMqMessageConverter {

    @Override
    public void prepare(Map config, TopologyContext context) {
        // your implementation here
    }

    @Override
    public String getExchange(Tuple tuple) throws Exception {
        // your implementation here
    }

    @Override
    public String getRoutingKey(Tuple tuple) throws Exception {
        // your implementation here
    }

    @Override
    public AMQP.BasicProperties getProperties(Tuple tuple) throws Exception {
        // your implementation here
    }

    @Override
    public byte[] getMessageBody(Tuple tuple) throws Exception {
        // your implementation here
    }

    @Override
    public void cleanup() {
        // your implementation here
    }

}
```

Далее вам необходимо передать вашу реализацию в RabbitMqBolt:

```java
MyTupleToRabbitMqMessageConverter converter = new MyTupleToRabbitMqMessageConverter();
RabbitMqBolt rabbitMqBolt = new RabbitMqBolt(converter);
```

Для RabbitMqBolt также можно указать различные параметры:

```java
builder.setBolt("rabbitmq-bolt", rabbitMqBolt)
       .addConfiguration(RabbitMqBolt.KEY_MANDATORY, false)
       .addConfiguration(RabbitMqBolt.KEY_IMMEDIATE, false);
```

Подробнее о параметрах для работы с RabbitMQ вы можете прочитать здесь: https://www.rabbitmq.com/amqp-0-9-1-reference.html
