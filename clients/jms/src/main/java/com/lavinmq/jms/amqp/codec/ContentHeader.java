package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.ProtocolException;

/** Payload of a content header frame: class id, weight (always 0), body size and the Basic properties. */
public record ContentHeader(int classId, long bodySize, BasicProperties properties) {
    public static final int BASIC_CLASS_ID = 60;

    public void write(AmqpWriter w) {
        w.short_(classId);
        w.short_(0);
        w.longlong(bodySize);
        properties.write(w);
    }

    public static ContentHeader read(AmqpReader r) throws ProtocolException {
        int classId = r.short_();
        int weight = r.short_();
        if (weight != 0) throw new ProtocolException("content header weight must be 0, got " + weight);
        long bodySize = r.longlong();
        if (bodySize < 0) throw new ProtocolException("negative body size");
        BasicProperties props = BasicProperties.read(r);
        return new ContentHeader(classId, bodySize, props);
    }
}
