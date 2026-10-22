package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.ProtocolException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AMQP 0-9-1 field table codec using the RabbitMQ/LavinMQ dialect of field types.
 *
 * <p>Write mapping: Boolean→t, Byte→b, Short→s, Integer→I, Long→l, Float→f, Double→d, String→S,
 * byte[]→x, List→A, Map→F, null→V, Instant/Date→T (seconds), BigDecimal→D.
 *
 * <p>Read mapping: t→Boolean, b→Byte, B→Short, s/U→Short, u→Integer, I→Integer, i→Long, l/L→Long,
 * f→Float, d→Double, D→BigDecimal, S→String, x→byte[], A→List, T→Instant, F→Map, V→null.
 */
public final class FieldTable {
    private FieldTable() {
    }

    public static void write(Map<String, Object> table, AmqpWriter w) {
        int lenPos = w.size();
        w.long_(0);
        int start = w.size();
        if (table != null) {
            for (Map.Entry<String, Object> e : table.entrySet()) {
                String key = e.getKey();
                if (key == null) throw new IllegalArgumentException("null field table key");
                if (key.getBytes(StandardCharsets.UTF_8).length > 128) {
                    throw new IllegalArgumentException("field table key longer than 128 bytes: " + key);
                }
                w.shortstr(key);
                writeValue(e.getValue(), w);
            }
        }
        w.patchLong(lenPos, w.size() - start);
    }

    public static void writeValue(Object v, AmqpWriter w) {
        if (v == null) {
            w.octet('V');
        } else if (v instanceof Boolean) {
            w.octet('t').octet((Boolean) v ? 1 : 0);
        } else if (v instanceof Byte) {
            w.octet('b').octet((Byte) v);
        } else if (v instanceof Short) {
            w.octet('s').int16((Short) v);
        } else if (v instanceof Integer) {
            w.octet('I').int32((Integer) v);
        } else if (v instanceof Long) {
            w.octet('l').longlong((Long) v);
        } else if (v instanceof Float) {
            w.octet('f').float_((Float) v);
        } else if (v instanceof Double) {
            w.octet('d').double_((Double) v);
        } else if (v instanceof String) {
            w.octet('S').longstr((String) v);
        } else if (v instanceof byte[]) {
            w.octet('x').longstr((byte[]) v);
        } else if (v instanceof Instant) {
            w.octet('T').timestamp(((Instant) v).getEpochSecond());
        } else if (v instanceof Date) {
            w.octet('T').timestamp(((Date) v).getTime() / 1000);
        } else if (v instanceof BigDecimal) {
            BigDecimal d = (BigDecimal) v;
            int scale = d.scale();
            if (scale < 0 || scale > 255) throw new IllegalArgumentException("decimal scale out of range: " + scale);
            BigInteger unscaled = d.unscaledValue();
            if (unscaled.bitLength() > 31) throw new IllegalArgumentException("decimal value does not fit in 32 bits: " + d);
            w.octet('D').octet(scale).int32(unscaled.intValue());
        } else if (v instanceof List) {
            w.octet('A');
            int lenPos = w.size();
            w.long_(0);
            int start = w.size();
            for (Object o : (List<?>) v) writeValue(o, w);
            w.patchLong(lenPos, w.size() - start);
        } else if (v instanceof Map) {
            w.octet('F');
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) v;
            write(m, w);
        } else {
            throw new IllegalArgumentException("unsupported field table value type: " + v.getClass().getName());
        }
    }

    public static Map<String, Object> read(AmqpReader r) throws ProtocolException {
        long len = r.long_();
        if (len > r.remaining()) throw new ProtocolException("field table length " + len + " exceeds payload");
        int end = r.position() + (int) len;
        Map<String, Object> table = new LinkedHashMap<>();
        while (r.position() < end) {
            String key = r.shortstr();
            table.put(key, readValue(r));
        }
        if (r.position() != end) throw new ProtocolException("field table overran its declared length");
        return table;
    }

    public static Object readValue(AmqpReader r) throws ProtocolException {
        int tag = r.octet();
        switch (tag) {
            case 't': return r.octet() != 0;
            case 'b': return (byte) r.octet();
            case 'B': return (short) r.octet();
            case 's':
            case 'U': return r.int16();
            case 'u': return r.short_();
            case 'I': return r.int32();
            case 'i': return r.long_();
            case 'l':
            case 'L': return r.longlong();
            case 'f': return r.float_();
            case 'd': return r.double_();
            case 'D': {
                int scale = r.octet();
                int unscaled = r.int32();
                return BigDecimal.valueOf(unscaled, scale);
            }
            case 'S': return r.longstrAsString();
            case 'x': return r.longstr();
            case 'A': {
                long len = r.long_();
                if (len > r.remaining()) throw new ProtocolException("field array length " + len + " exceeds payload");
                int end = r.position() + (int) len;
                List<Object> list = new ArrayList<>();
                while (r.position() < end) list.add(readValue(r));
                if (r.position() != end) throw new ProtocolException("field array overran its declared length");
                return list;
            }
            case 'T': return Instant.ofEpochSecond(r.longlong());
            case 'F': return read(r);
            case 'V': return null;
            default: throw new ProtocolException("unknown field table value tag: " + (char) tag + " (0x" + Integer.toHexString(tag) + ")");
        }
    }
}
