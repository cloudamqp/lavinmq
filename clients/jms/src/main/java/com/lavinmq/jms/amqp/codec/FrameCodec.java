package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.method.Method;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Reads and writes frame envelopes and splits message bodies into body frames. */
public final class FrameCodec {
    /** Bytes of envelope per frame: 1 type + 2 channel + 4 size + 1 end. */
    public static final int FRAME_OVERHEAD = 8;
    public static final byte[] PROTOCOL_HEADER = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};

    private FrameCodec() {
    }

    /**
     * Reads one frame. {@code maxFrameSize} 0 disables the size check.
     *
     * @throws EOFException when the stream ends between frames
     * @throws ProtocolException on a malformed frame
     */
    public static Frame read(DataInputStream in, int maxFrameSize) throws IOException {
        int type = in.read();
        if (type < 0) throw new EOFException("connection closed by peer");
        if (type == 'A') {
            byte[] rest = new byte[7];
            in.readFully(rest);
            throw new ProtocolException("server does not speak AMQP 0-9-1 (replied with protocol header AMQP "
                + (rest[3] & 0xFF) + "." + (rest[4] & 0xFF) + "." + (rest[5] & 0xFF) + "." + (rest[6] & 0xFF) + ")");
        }
        if (type != FrameType.METHOD && type != FrameType.HEADER && type != FrameType.BODY && type != FrameType.HEARTBEAT) {
            throw new ProtocolException("unknown frame type " + type);
        }
        int channel = in.readUnsignedShort();
        long size = in.readInt() & 0xFFFFFFFFL;
        if (maxFrameSize > 0 && size + FRAME_OVERHEAD > maxFrameSize) {
            throw new ProtocolException("frame of " + (size + FRAME_OVERHEAD) + " bytes exceeds negotiated frame max " + maxFrameSize);
        }
        if (size > Integer.MAX_VALUE - FRAME_OVERHEAD) throw new ProtocolException("frame too large: " + size);
        byte[] payload = new byte[(int) size];
        in.readFully(payload);
        int end = in.read();
        if (end < 0) throw new EOFException("connection closed by peer");
        if (end != FrameType.END) throw new ProtocolException("bad frame end octet 0x" + Integer.toHexString(end));
        return new Frame(type, channel, payload);
    }

    public static void write(DataOutputStream out, Frame f) throws IOException {
        out.write(f.type());
        out.writeShort(f.channel());
        out.writeInt(f.payload().length);
        out.write(f.payload());
        out.write(FrameType.END);
    }

    /** Splits a body into frames of at most {@code frameMax - FRAME_OVERHEAD} bytes; an empty body yields no frames. */
    public static List<Frame> splitBody(int channel, byte[] body, int frameMax) {
        List<Frame> frames = new ArrayList<>();
        int chunk = bodyChunkSize(frameMax);
        for (int off = 0; off < body.length; off += chunk) {
            int len = Math.min(chunk, body.length - off);
            frames.add(Frame.body(channel, body, off, len));
        }
        return frames;
    }

    private static int bodyChunkSize(int frameMax) {
        if (frameMax <= 0) return Integer.MAX_VALUE - FRAME_OVERHEAD;
        if (frameMax <= FRAME_OVERHEAD) throw new IllegalArgumentException("frameMax too small: " + frameMax);
        return frameMax - FRAME_OVERHEAD;
    }

    /** Writes a content-bearing method, its header and body frames without intermediate frame objects. */
    public static void writeContent(DataOutputStream out, int channel, Method m, BasicProperties props, byte[] body, int frameMax)
        throws IOException {
        write(out, Frame.method(channel, m));
        write(out, Frame.header(channel, body.length, props));
        int chunk = bodyChunkSize(frameMax);
        for (int off = 0; off < body.length; off += chunk) {
            int len = Math.min(chunk, body.length - off);
            out.write(FrameType.BODY);
            out.writeShort(channel);
            out.writeInt(len);
            out.write(body, off, len);
            out.write(FrameType.END);
        }
    }
}
