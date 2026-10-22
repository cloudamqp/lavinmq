package com.lavinmq.jms.amqp;

import java.io.IOException;

/** Base class for every error raised by the AMQP transport. */
public class AmqpException extends IOException {
    private static final long serialVersionUID = 1L;

    public AmqpException(String message) {
        super(message);
    }

    public AmqpException(String message, Throwable cause) {
        super(message, cause);
    }

    /** AMQP reply code when the error originated from a close method, otherwise 0. */
    public int replyCode() {
        return 0;
    }

    /** AMQP reply text when the error originated from a close method, otherwise null. */
    public String replyText() {
        return null;
    }
}
