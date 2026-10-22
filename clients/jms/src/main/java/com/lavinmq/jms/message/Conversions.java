package com.lavinmq.jms.message;

import jakarta.jms.MessageFormatException;

/** The JMS type-conversion table shared by message properties, MapMessage and StreamMessage. */
final class Conversions {
    private Conversions() {
    }

    static boolean toBoolean(Object v) throws MessageFormatException {
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof String) return Boolean.parseBoolean((String) v);
        throw incompatible(v, "boolean");
    }

    static byte toByte(Object v) throws MessageFormatException {
        if (v == null) return Byte.valueOf((String) null);
        if (v instanceof Byte) return (Byte) v;
        if (v instanceof String) return Byte.parseByte((String) v);
        throw incompatible(v, "byte");
    }

    static short toShort(Object v) throws MessageFormatException {
        if (v == null) return Short.valueOf((String) null);
        if (v instanceof Byte) return (Byte) v;
        if (v instanceof Short) return (Short) v;
        if (v instanceof String) return Short.parseShort((String) v);
        throw incompatible(v, "short");
    }

    static char toChar(Object v) throws MessageFormatException {
        if (v == null) throw new NullPointerException("null cannot be converted to char");
        if (v instanceof Character) return (Character) v;
        throw incompatible(v, "char");
    }

    static int toInt(Object v) throws MessageFormatException {
        if (v == null) return Integer.valueOf((String) null);
        if (v instanceof Byte) return (Byte) v;
        if (v instanceof Short) return (Short) v;
        if (v instanceof Integer) return (Integer) v;
        if (v instanceof String) return Integer.parseInt((String) v);
        throw incompatible(v, "int");
    }

    static long toLong(Object v) throws MessageFormatException {
        if (v == null) return Long.valueOf((String) null);
        if (v instanceof Byte) return (Byte) v;
        if (v instanceof Short) return (Short) v;
        if (v instanceof Integer) return (Integer) v;
        if (v instanceof Long) return (Long) v;
        if (v instanceof String) return Long.parseLong((String) v);
        throw incompatible(v, "long");
    }

    static float toFloat(Object v) throws MessageFormatException {
        if (v == null) return Float.valueOf((String) null);
        if (v instanceof Float) return (Float) v;
        if (v instanceof String) return Float.parseFloat((String) v);
        throw incompatible(v, "float");
    }

    static double toDouble(Object v) throws MessageFormatException {
        if (v == null) return Double.valueOf((String) null);
        if (v instanceof Float) return (Float) v;
        if (v instanceof Double) return (Double) v;
        if (v instanceof String) return Double.parseDouble((String) v);
        throw incompatible(v, "double");
    }

    static String toString(Object v) throws MessageFormatException {
        if (v == null) return null;
        if (v instanceof byte[]) throw incompatible(v, "String");
        return v.toString();
    }

    static byte[] toBytes(Object v) throws MessageFormatException {
        if (v == null) return null;
        if (v instanceof byte[]) return ((byte[]) v).clone();
        throw incompatible(v, "byte[]");
    }

    /** Validates an object for setObjectProperty / MapMessage.setObject / StreamMessage.writeObject. */
    static Object checkObject(Object v, boolean allowBytesAndChar) throws MessageFormatException {
        if (v == null) return null;
        if (v instanceof Boolean || v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long
            || v instanceof Float || v instanceof Double || v instanceof String) {
            return v;
        }
        if (allowBytesAndChar && (v instanceof Character || v instanceof byte[])) {
            return v instanceof byte[] ? ((byte[]) v).clone() : v;
        }
        throw new MessageFormatException("unsupported value type " + v.getClass().getName());
    }

    private static MessageFormatException incompatible(Object v, String target) {
        return new MessageFormatException("cannot convert " + v.getClass().getSimpleName() + " to " + target);
    }
}
