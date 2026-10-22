package com.lavinmq.jms.amqp;

/** Malformed or unexpected data on the wire. Always a hard (connection-level) error. */
public class ProtocolException extends AmqpException {
    private static final long serialVersionUID = 1L;

    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
