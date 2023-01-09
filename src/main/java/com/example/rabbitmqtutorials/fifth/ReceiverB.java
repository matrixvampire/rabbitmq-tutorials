package com.example.rabbitmqtutorials.fifth;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReceiverB {

  private static final String EXCHANGE_NAME = "fifth";

  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("localhost");
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
    String queueName = channel.queueDeclare().getQueue();
    String routingkey = "b.*";
    channel.queueBind(queueName, EXCHANGE_NAME, routingkey);

    log.info("[*] Waiting for messages. To exit press Ctrl + C");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      log.info("[x] Received '{}' : '{}'", delivery.getEnvelope().getRoutingKey(), message);
    };
    channel.basicConsume(queueName, true, deliverCallback, (consumerTag, sig) -> {
    });
  }
}
