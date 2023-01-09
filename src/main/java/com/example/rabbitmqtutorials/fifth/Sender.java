package com.example.rabbitmqtutorials.fifth;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Sender {

  private static final String EXCHANGE_NAME = "fifth";

  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("localhost");
    try (Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
      for (int i = 1; i <= 100; i++) {
        String message = "Message " + i;
        String routingKey = i % 3 == 0 ? "a.3" : "b.3";
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes(StandardCharsets.UTF_8));
        log.info("[x] Sent '{}'", message);
      }
    }
  }
}
