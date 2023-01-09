package com.example.rabbitmqtutorials.fourth;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReceiverA {

  private static final String EXCHANGE_NAME = "fourth";

  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("localhost");
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    String queueName = channel.queueDeclare().getQueue();
    String routingkey = "a";
    channel.queueBind(queueName, EXCHANGE_NAME, routingkey);

    log.info("[*] Waiting for messages. To exit press Ctrl + C");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      log.info("[x] Received '{}'", message);
    };
    channel.basicConsume(queueName, true, deliverCallback, (consumerTag, sig) -> {
    });
  }
}
