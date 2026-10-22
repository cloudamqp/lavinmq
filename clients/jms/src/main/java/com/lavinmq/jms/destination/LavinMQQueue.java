package com.lavinmq.jms.destination;

import jakarta.jms.Queue;

/** A JMS queue backed by an AMQP queue of the same name on the default exchange. */
public class LavinMQQueue extends LavinMQDestination implements Queue {
    private static final long serialVersionUID = 1L;

    public LavinMQQueue(String name) {
        this(Kind.QUEUE, name);
    }

    protected LavinMQQueue(Kind kind, String name) {
        super(kind, name, null);
    }

    @Override
    public String getQueueName() {
        return name();
    }
}
