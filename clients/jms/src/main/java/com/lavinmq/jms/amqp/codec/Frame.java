package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.method.Method;
import java.util.Arrays;

/** One AMQP frame: type, channel and payload (without the 7-byte header and the end octet). */
public record Frame(int type, int channel, byte[] payload) {
    public static Frame method(int channel, Method m) {
        AmqpWriter w = new AmqpWriter();
        w.short_(m.classId());
        w.short_(m.methodId());
        m.writeArguments(w);
        return new Frame(FrameType.METHOD, channel, w.toByteArray());
    }

    public static Frame header(int channel, long bodySize, BasicProperties props) {
        AmqpWriter w = new AmqpWriter();
        new ContentHeader(ContentHeader.BASIC_CLASS_ID, bodySize, props).write(w);
        return new Frame(FrameType.HEADER, channel, w.toByteArray());
    }

    public static Frame body(int channel, byte[] body, int off, int len) {
        return new Frame(FrameType.BODY, channel, Arrays.copyOfRange(body, off, off + len));
    }

    public static Frame heartbeat() {
        return new Frame(FrameType.HEARTBEAT, 0, new byte[0]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Frame)) return false;
        Frame f = (Frame) o;
        return type == f.type && channel == f.channel && Arrays.equals(payload, f.payload);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * type + channel) + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "Frame{type=" + type + ", channel=" + channel + ", size=" + payload.length + '}';
    }
}
