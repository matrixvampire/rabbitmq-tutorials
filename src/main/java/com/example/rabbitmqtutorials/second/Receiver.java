package com.example.rabbitmqtutorials.second;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Receiver {

  private static final String QUEUE_NAME = "second";
  private static final Random RANDOM = new Random();

  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost("localhost");
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(QUEUE_NAME, true, false, false, null);
    log.info("[*] Waiting for messages. To exit press Ctrl + C");

    channel.basicQos(1);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      log.info("[x] Received '{}'", message);
      try {
        doWork();
      } finally {
        log.info("[x] Done");
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), true);
      }
    };
    channel.basicConsume(QUEUE_NAME, false, deliverCallback, (consumerTag, sig) -> {
    });
  }

  private static void doWork() {
    try {
      Thread.sleep(RANDOM.nextInt(3) * 1000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
