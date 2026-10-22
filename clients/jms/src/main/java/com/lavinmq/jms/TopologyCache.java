package com.lavinmq.jms;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Remembers what this connection has already declared on the broker so it is done once per connection. */
final class TopologyCache {
    final Set<String> queues = ConcurrentHashMap.newKeySet();
    final Set<String> delayedQueueBindings = ConcurrentHashMap.newKeySet();
    volatile boolean topicExchange;
    volatile boolean delayedQueueExchange;
    volatile boolean delayedTopicExchange;

    void clear() {
        queues.clear();
        delayedQueueBindings.clear();
        topicExchange = false;
        delayedQueueExchange = false;
        delayedTopicExchange = false;
    }
}
