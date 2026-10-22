package com.lavinmq.jms.amqp;

/** The channel was closed (by either peer) while an operation was pending or attempted. */
public class ChannelClosedException extends AmqpException {
    private static final long serialVersionUID = 1L;
    private final transient ShutdownSignal signal;

    public ChannelClosedException(ShutdownSignal signal) {
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
