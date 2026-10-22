package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.codec.BasicProperties;

/** An unroutable mandatory message returned by the server with basic.return. */
public record ReturnedMessage(int replyCode, String replyText, String exchange, String routingKey,
                              BasicProperties properties, byte[] body) {
}
