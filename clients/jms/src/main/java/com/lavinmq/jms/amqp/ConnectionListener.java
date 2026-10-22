package com.lavinmq.jms.amqp;

/** Connection-wide events, invoked on a dispatch thread. */
public interface ConnectionListener {
    default void onBlocked(String reason) {
    }

    default void onUnblocked() {
    }

    /** Called exactly once when the connection is gone, after all channel shutdown callbacks. */
    default void onShutdown(ShutdownSignal signal) {
    }
}
