package com.lavinmq.jms.amqp.codec;

/** AMQP 0-9-1 frame type constants. */
public final class FrameType {
    public static final int METHOD = 1;
    public static final int HEADER = 2;
    public static final int BODY = 3;
    public static final int HEARTBEAT = 8;
    public static final int END = 0xCE;

    private FrameType() {
    }
}
