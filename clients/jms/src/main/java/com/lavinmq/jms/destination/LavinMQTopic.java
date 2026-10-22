package com.lavinmq.jms.destination;

import jakarta.jms.Topic;

/** A JMS topic backed by a routing key on the provider's topic exchange. */
public class LavinMQTopic extends LavinMQDestination implements Topic {
    private static final long serialVersionUID = 1L;

    public LavinMQTopic(String name) {
        super(Kind.TOPIC, name, null);
    }

    /** A topic received from a foreign AMQP exchange; sends go back to that exchange. */
    public LavinMQTopic(String name, String exchange) {
        super(Kind.TOPIC, name, exchange);
    }

    protected LavinMQTopic(Kind kind, String name) {
        super(kind, name, null);
    }

    @Override
    public String getTopicName() {
        return name();
    }
}
