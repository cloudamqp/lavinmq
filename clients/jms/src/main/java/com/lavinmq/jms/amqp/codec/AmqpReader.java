package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.ProtocolException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Big-endian reader over a byte array. Underflow raises {@link ProtocolException}. */
public final class AmqpReader {
    private final byte[] buf;
    private int pos;
    private final int limit;

    public AmqpReader(byte[] buf) {
        this(buf, 0, buf.length);
    }

    public AmqpReader(byte[] buf, int off, int len) {
        this.buf = buf;
        this.pos = off;
        this.limit = off + len;
    }

    private void need(int n) throws ProtocolException {
        if (pos + n > limit) throw new ProtocolException("truncated frame payload: need " + n + " more bytes, have " + (limit - pos));
    }

    public int octet() throws ProtocolException {
        need(1);
        return buf[pos++] & 0xFF;
    }

    public int short_() throws ProtocolException {
        need(2);
        int v = ((buf[pos] & 0xFF) << 8) | (buf[pos + 1] & 0xFF);
        pos += 2;
        return v;
    }

    public long long_() throws ProtocolException {
        need(4);
        long v = ((long) (buf[pos] & 0xFF) << 24) | ((buf[pos + 1] & 0xFF) << 16) | ((buf[pos + 2] & 0xFF) << 8) | (buf[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    public int int32() throws ProtocolException {
        return (int) long_();
    }

    public short int16() throws ProtocolException {
        return (short) short_();
    }

    public long longlong() throws ProtocolException {
        need(8);
        long v = 0;
        for (int i = 0; i < 8; i++) v = (v << 8) | (buf[pos++] & 0xFF);
        return v;
    }

    public float float_() throws ProtocolException {
        return Float.intBitsToFloat(int32());
    }

    public double double_() throws ProtocolException {
        return Double.longBitsToDouble(longlong());
    }

    public String shortstr() throws ProtocolException {
        int len = octet();
        need(len);
        String s = new String(buf, pos, len, StandardCharsets.UTF_8);
        pos += len;
        return s;
    }

    public byte[] longstr() throws ProtocolException {
        long len = long_();
        if (len > Integer.MAX_VALUE) throw new ProtocolException("long string too large: " + len);
        need((int) len);
        byte[] b = new byte[(int) len];
        System.arraycopy(buf, pos, b, 0, b.length);
        pos += b.length;
        return b;
    }

    public String longstrAsString() throws ProtocolException {
        return new String(longstr(), StandardCharsets.UTF_8);
    }

    public byte[] bytes(int len) throws ProtocolException {
        need(len);
        byte[] b = new byte[len];
        System.arraycopy(buf, pos, b, 0, len);
        pos += len;
        return b;
    }

    public Map<String, Object> table() throws ProtocolException {
        return FieldTable.read(this);
    }

    public long timestamp() throws ProtocolException {
        return longlong();
    }

    /** Reads one octet and unpacks n booleans, LSB first. */
    public boolean[] bits(int n) throws ProtocolException {
        int v = octet();
        boolean[] out = new boolean[n];
        for (int i = 0; i < n; i++) out[i] = (v & (1 << i)) != 0;
        return out;
    }

    public int remaining() {
        return limit - pos;
    }

    public int position() {
        return pos;
    }

    public void skip(int n) throws ProtocolException {
        need(n);
        pos += n;
    }
}
