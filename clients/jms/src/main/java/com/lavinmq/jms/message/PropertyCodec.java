package com.lavinmq.jms.message;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Converts between JMS property values and AMQP field-table values. */
public final class PropertyCodec {
    private PropertyCodec() {
    }

    /** JMS property values are already valid table values (boolean, byte, short, int, long, float, double, String). */
    public static Object toAmqp(Object v) {
        return v;
    }

    /** Values received in a headers table that have no JMS counterpart are narrowed to strings or longs. */
    public static Object fromAmqp(Object v) {
        if (v == null || v instanceof Boolean || v instanceof Byte || v instanceof Short || v instanceof Integer
            || v instanceof Long || v instanceof Float || v instanceof Double || v instanceof String) {
            return v;
        }
        if (v instanceof Instant) return ((Instant) v).toEpochMilli();
        if (v instanceof byte[]) return new String((byte[]) v, java.nio.charset.StandardCharsets.UTF_8);
        if (v instanceof BigDecimal || v instanceof List || v instanceof Map) return v.toString();
        return v.toString();
    }
}
