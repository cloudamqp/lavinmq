package com.lavinmq.jms.message;

import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

public final class LavinMQMapMessage extends LavinMQMessage implements MapMessage {
    private final Map<String, Object> map = new LinkedHashMap<>();

    public LavinMQMapMessage() {
    }

    public LavinMQMapMessage(Map<String, Object> values) {
        map.putAll(values);
    }

    public Map<String, Object> mapView() {
        return Collections.unmodifiableMap(map);
    }

    private void put(String name, Object value) throws JMSException {
        checkBodyWriteable();
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("map item name must not be empty");
        map.put(name, value);
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return Conversions.toBoolean(map.get(name));
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return Conversions.toByte(map.get(name));
    }

    @Override
    public short getShort(String name) throws JMSException {
        return Conversions.toShort(map.get(name));
    }

    @Override
    public char getChar(String name) throws JMSException {
        return Conversions.toChar(map.get(name));
    }

    @Override
    public int getInt(String name) throws JMSException {
        return Conversions.toInt(map.get(name));
    }

    @Override
    public long getLong(String name) throws JMSException {
        return Conversions.toLong(map.get(name));
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return Conversions.toFloat(map.get(name));
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return Conversions.toDouble(map.get(name));
    }

    @Override
    public String getString(String name) throws JMSException {
        return Conversions.toString(map.get(name));
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return Conversions.toBytes(map.get(name));
    }

    @Override
    public Object getObject(String name) {
        Object v = map.get(name);
        return v instanceof byte[] ? ((byte[]) v).clone() : v;
    }

    @Override
    public Enumeration<String> getMapNames() {
        return Collections.enumeration(new java.util.ArrayList<>(map.keySet()));
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        put(name, value == null ? null : value.clone());
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        byte[] copy = new byte[length];
        System.arraycopy(value, offset, copy, 0, length);
        put(name, copy);
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        put(name, Conversions.checkObject(value, true));
    }

    @Override
    public boolean itemExists(String name) {
        return map.containsKey(name);
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        map.clear();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (map.isEmpty()) return null;
        if (c.isAssignableFrom(Map.class)) return (T) new LinkedHashMap<>(map);
        throw bodyMismatch(c, "Map");
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        return map.isEmpty() || c.isAssignableFrom(Map.class);
    }

    @Override
    public String messageType() {
        return "map";
    }
}
