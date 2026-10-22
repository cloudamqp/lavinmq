package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.codec.BasicProperties;

/** A message delivered to a consumer by basic.deliver. */
public record Delivery(String consumerTag, long deliveryTag, boolean redelivered, String exchange, String routingKey,
                       BasicProperties properties, byte[] body) {
}
