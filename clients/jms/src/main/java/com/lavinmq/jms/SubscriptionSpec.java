package com.lavinmq.jms;

import com.lavinmq.jms.destination.DestinationNames;
import com.lavinmq.jms.destination.LavinMQDestination;

/** How a consumer maps to an AMQP queue: which queue, how it is declared and whether the consume is exclusive. */
final class SubscriptionSpec {
    enum Kind { QUEUE, TOPIC_UNSHARED, TOPIC_SHARED, DURABLE_UNSHARED, DURABLE_SHARED }

    final Kind kind;
    final LavinMQDestination destination;
    final String subscriptionName;
    final String clientId;

    SubscriptionSpec(Kind kind, LavinMQDestination destination, String subscriptionName, String clientId) {
        this.kind = kind;
        this.destination = destination;
        this.subscriptionName = subscriptionName;
        this.clientId = clientId;
    }

    boolean isDurable() {
        return kind == Kind.DURABLE_SHARED || kind == Kind.DURABLE_UNSHARED;
    }

    boolean isShared() {
        return kind == Kind.DURABLE_SHARED || kind == Kind.TOPIC_SHARED;
    }

    /** Only unshared durable subscriptions consume exclusively (so a second active subscriber is refused). */
    boolean exclusiveConsume() {
        return kind == Kind.DURABLE_UNSHARED;
    }

    /** The queue name, or null for a server-named queue. */
    String queueName() {
        switch (kind) {
            case QUEUE: return destination.name();
            case TOPIC_UNSHARED: return null;
            case TOPIC_SHARED: return DestinationNames.sharedQueue(clientId, subscriptionName);
            case DURABLE_UNSHARED:
            case DURABLE_SHARED: return DestinationNames.durableQueue(clientId, subscriptionName);
            default: throw new IllegalStateException();
        }
    }
}
