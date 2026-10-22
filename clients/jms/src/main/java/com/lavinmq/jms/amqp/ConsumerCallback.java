package com.lavinmq.jms.amqp;

/** Per-consumer callbacks, invoked in order on the channel's dispatch thread. */
public interface ConsumerCallback {
    void handleDelivery(Delivery delivery);

    /** The server cancelled the consumer (for example because its queue was deleted). */
    default void handleCancel(String consumerTag) {
    }

    /** The channel (or the whole connection) was closed; no more deliveries will arrive. */
    default void handleShutdown(String consumerTag, ShutdownSignal signal) {
    }
}
