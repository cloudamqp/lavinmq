package com.lavinmq.jms.amqp;

/** No reply to a synchronous method arrived within the configured RPC timeout. */
public class RpcTimeoutException extends AmqpException {
    private static final long serialVersionUID = 1L;

    public RpcTimeoutException(String message) {
        super(message);
    }
}
