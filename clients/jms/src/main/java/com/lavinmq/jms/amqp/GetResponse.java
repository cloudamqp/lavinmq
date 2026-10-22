package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.codec.BasicProperties;

/** A message fetched with basic.get. */
public record GetResponse(long deliveryTag, boolean redelivered, String exchange, String routingKey, long messageCount,
                          BasicProperties properties, byte[] body) {
}
