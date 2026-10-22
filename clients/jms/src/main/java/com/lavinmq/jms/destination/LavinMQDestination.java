package com.lavinmq.jms.destination;

import jakarta.jms.Destination;
import java.io.Serializable;
import java.util.Objects;

/** Base class of every destination produced by this provider. Destinations are plain values; nothing is declared on creation. */
public abstract class LavinMQDestination implements Destination, Serializable {
    private static final long serialVersionUID = 1L;

    public enum Kind { QUEUE, TOPIC, TEMP_QUEUE, TEMP_TOPIC }

    private volatile String name;
    private final Kind kind;
    /** Exchange a foreign AMQP message was received from, or null for destinations of this provider. */
    private final String exchange;

    protected LavinMQDestination(Kind kind, String name, String exchange) {
        this.kind = kind;
        this.name = name;
        this.exchange = exchange;
    }

    public String name() {
        return name;
    }

    /** Server-named temporary queues get a new name after a recovery. */
    protected void rename(String newName) {
        this.name = newName;
    }

    public Kind kind() {
        return kind;
    }

    public String exchange() {
        return exchange;
    }

    public boolean isQueueKind() {
        return kind == Kind.QUEUE || kind == Kind.TEMP_QUEUE;
    }

    public boolean isTopicKind() {
        return kind == Kind.TOPIC || kind == Kind.TEMP_TOPIC;
    }

    public boolean isTemporary() {
        return kind == Kind.TEMP_QUEUE || kind == Kind.TEMP_TOPIC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LavinMQDestination)) return false;
        LavinMQDestination d = (LavinMQDestination) o;
        return kind == d.kind && name.equals(d.name) && Objects.equals(exchange, d.exchange);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, name, exchange);
    }

    @Override
    public String toString() {
        return kind.name().toLowerCase().replace('_', '-') + "://" + name;
    }
}
