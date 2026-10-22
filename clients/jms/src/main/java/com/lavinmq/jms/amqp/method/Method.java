package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.codec.AmqpWriter;

/** An AMQP 0-9-1 method with its arguments. */
public interface Method {
    int classId();

    int methodId();

    String name();

    void writeArguments(AmqpWriter w);

    /** True when the method is followed by a content header and body frames. */
    default boolean hasContent() {
        return false;
    }
}
