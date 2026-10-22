package com.lavinmq.jms.amqp.codec;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/** Growable big-endian byte sink for AMQP 0-9-1 encoding. Not thread-safe. */
public final class AmqpWriter {
    private byte[] buf;
    private int pos;

    public AmqpWriter() {
        this(64);
    }

    public AmqpWriter(int initialCapacity) {
        buf = new byte[Math.max(8, initialCapacity)];
    }

    private void ensure(int n) {
        if (pos + n > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + n));
        }
    }

    public AmqpWriter octet(int v) {
        ensure(1);
        buf[pos++] = (byte) v;
        return this;
    }

    /** Unsigned 16-bit. */
    public AmqpWriter short_(int v) {
        if (v < 0 || v > 0xFFFF) throw new IllegalArgumentException("short out of range: " + v);
        ensure(2);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    /** Unsigned 32-bit. */
    public AmqpWriter long_(long v) {
        if (v < 0 || v > 0xFFFFFFFFL) throw new IllegalArgumentException("long out of range: " + v);
        ensure(4);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    public AmqpWriter longlong(long v) {
        ensure(8);
        for (int i = 7; i >= 0; i--) buf[pos++] = (byte) (v >>> (i * 8));
        return this;
    }

    public AmqpWriter int32(int v) {
        ensure(4);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    public AmqpWriter int16(short v) {
        ensure(2);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    public AmqpWriter float_(float v) {
        return int32(Float.floatToIntBits(v));
    }

    public AmqpWriter double_(double v) {
        return longlong(Double.doubleToLongBits(v));
    }

    public AmqpWriter shortstr(String s) {
        if (s == null) s = "";
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > 255) throw new IllegalArgumentException("short string longer than 255 bytes: " + b.length);
        ensure(1 + b.length);
        buf[pos++] = (byte) b.length;
        System.arraycopy(b, 0, buf, pos, b.length);
        pos += b.length;
        return this;
    }

    public AmqpWriter longstr(byte[] b) {
        if (b == null) b = new byte[0];
        long_(b.length);
        bytes(b, 0, b.length);
        return this;
    }

    public AmqpWriter longstr(String s) {
        return longstr(s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8));
    }

    public AmqpWriter bytes(byte[] b, int off, int len) {
        ensure(len);
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
        return this;
    }

    public AmqpWriter table(Map<String, Object> table) {
        FieldTable.write(table, this);
        return this;
    }

    /** Seconds since the epoch, 64 bit. */
    public AmqpWriter timestamp(long seconds) {
        return longlong(seconds);
    }

    /** Packs up to 8 consecutive booleans into one octet, LSB first. */
    public AmqpWriter bits(boolean... flags) {
        if (flags.length > 8) throw new IllegalArgumentException("at most 8 bits per octet");
        int v = 0;
        for (int i = 0; i < flags.length; i++) if (flags[i]) v |= 1 << i;
        return octet(v);
    }

    public int size() {
        return pos;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, pos);
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.write(buf, 0, pos);
    }

    /** Overwrite a previously written 32-bit length at the given position (used for table/array length prefixes). */
    void patchLong(int at, long v) {
        buf[at] = (byte) (v >>> 24);
        buf[at + 1] = (byte) (v >>> 16);
        buf[at + 2] = (byte) (v >>> 8);
        buf[at + 3] = (byte) v;
    }
}
