package com.lavinmq.jms.amqp;

/** The connection was closed (by either peer or by a socket error) while an operation was pending or attempted. */
public class ConnectionClosedException extends AmqpException {
    private static final long serialVersionUID = 1L;
    private final transient ShutdownSignal signal;

    public ConnectionClosedException(ShutdownSignal signal) {
        super(signal.toString(), signal.cause());
        this.signal = signal;
    }

    public ShutdownSignal signal() {
        return signal;
    }

    @Override
    public int replyCode() {
        return signal.replyCode();
    }

    @Override
    public String replyText() {
        return signal.replyText();
    }
}
