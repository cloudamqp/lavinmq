package com.lavinmq.jms.amqp;

/** Channel-wide events, invoked in order on the channel's dispatch thread. */
public interface ChannelListener {
    default void onReturn(ReturnedMessage returned) {
    }

    /** Publisher confirm from the server. */
    default void onConfirm(boolean ack, long deliveryTag, boolean multiple) {
    }

    default void onFlow(boolean active) {
    }

    default void onShutdown(ShutdownSignal signal) {
    }
}
