package com.lavinmq.jms;

/** When a synchronous {@code send} waits for the broker's publisher confirm. */
public enum PublisherConfirmMode {
    /** Wait only for messages sent with {@code DeliveryMode.PERSISTENT} (the default). */
    PERSISTENT,
    /** Wait for every message. */
    ALWAYS,
    /** Never wait; confirms are still tracked for asynchronous sends. */
    NEVER
}
