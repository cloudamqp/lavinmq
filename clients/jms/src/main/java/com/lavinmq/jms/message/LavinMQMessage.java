package com.lavinmq.jms.message;

import com.lavinmq.jms.destination.LavinMQDestination;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** A message with no body; base class of the five body-bearing message types. */
public class LavinMQMessage implements Message {
    /** Hook the consumer installs so that {@link #acknowledge()} reaches the session. */
    public interface AckHandle {
        void acknowledge() throws JMSException;
    }

    private static final Set<String> RESERVED = Set.of("NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE");

    private String messageId;
    private long timestamp;
    private String correlationId;
    private Destination replyTo;
    private Destination destination;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private boolean redelivered;
    private String type;
    private long expiration;
    private long deliveryTime;
    private int priority = Message.DEFAULT_PRIORITY;
    private final Map<String, Object> properties = new LinkedHashMap<>();
    private boolean propertiesReadOnly;
    protected boolean bodyReadOnly;
    private transient AckHandle ackHandle;

    public LavinMQMessage() {
    }

    // ---- headers --------------------------------------------------------------------------------------------------

    @Override
    public String getJMSMessageID() {
        return messageId;
    }

    @Override
    public void setJMSMessageID(String id) {
        this.messageId = id;
    }

    @Override
    public long getJMSTimestamp() {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        throw new UnsupportedOperationException("byte correlation ids are not supported");
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) {
        throw new UnsupportedOperationException("byte correlation ids are not supported");
    }

    @Override
    public void setJMSCorrelationID(String correlationID) {
        this.correlationId = correlationID;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationId;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        if (replyTo != null && !(replyTo instanceof LavinMQDestination)) {
            throw new jakarta.jms.InvalidDestinationException("reply-to destination is not a LavinMQ destination: " + replyTo);
        }
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSException("invalid delivery mode " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public void setJMSType(String type) {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) {
        this.expiration = expiration;
    }

    @Override
    public long getJMSDeliveryTime() {
        return deliveryTime;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) {
        this.deliveryTime = deliveryTime;
    }

    @Override
    public int getJMSPriority() {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        if (priority < 0 || priority > 9) throw new JMSException("priority out of range: " + priority);
        this.priority = priority;
    }

    // ---- properties -----------------------------------------------------------------------------------------------

    @Override
    public void clearProperties() {
        properties.clear();
        propertiesReadOnly = false;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return Conversions.toBoolean(properties.get(name));
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return Conversions.toByte(properties.get(name));
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return Conversions.toShort(properties.get(name));
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return Conversions.toInt(properties.get(name));
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return Conversions.toLong(properties.get(name));
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return Conversions.toFloat(properties.get(name));
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return Conversions.toDouble(properties.get(name));
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object v = properties.get(name);
        return v == null ? null : v.toString();
    }

    @Override
    public Object getObjectProperty(String name) {
        Object v = properties.get(name);
        return v instanceof byte[] ? ((byte[]) v).clone() : v;
    }

    @Override
    public Enumeration<String> getPropertyNames() {
        return Collections.enumeration(new java.util.ArrayList<>(properties.keySet()));
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        put(name, Conversions.checkObject(value, false));
    }

    private void put(String name, Object value) throws JMSException {
        if (propertiesReadOnly) throw new MessageNotWriteableException("message properties are read-only");
        validatePropertyName(name);
        properties.put(name, value);
    }

    static void validatePropertyName(String name) {
        if (name == null || name.isEmpty()) throw new IllegalArgumentException("property name must not be empty");
        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            throw new IllegalArgumentException("invalid property name: " + name);
        }
        for (int i = 1; i < name.length(); i++) {
            if (!Character.isJavaIdentifierPart(name.charAt(i))) throw new IllegalArgumentException("invalid property name: " + name);
        }
        if (RESERVED.contains(name.toUpperCase(Locale.ROOT))) throw new IllegalArgumentException("reserved property name: " + name);
    }

    /** Provider-side access: stores a property without validation (for received headers such as x-death). */
    public void setPropertyInternal(String name, Object value) {
        properties.put(name, value);
    }

    public Map<String, Object> propertiesView() {
        return Collections.unmodifiableMap(properties);
    }

    public void setPropertiesReadOnly(boolean readOnly) {
        this.propertiesReadOnly = readOnly;
    }

    public void setBodyReadOnly(boolean readOnly) {
        this.bodyReadOnly = readOnly;
    }

    protected void checkBodyWriteable() throws MessageNotWriteableException {
        if (bodyReadOnly) throw new MessageNotWriteableException("message body is read-only");
    }

    // ---- acknowledgement & body ------------------------------------------------------------------------------------

    public void setAckHandle(AckHandle handle) {
        this.ackHandle = handle;
    }

    @Override
    public void acknowledge() throws JMSException {
        AckHandle h = ackHandle;
        if (h != null) h.acknowledge();
    }

    @Override
    public void clearBody() throws JMSException {
        bodyReadOnly = false;
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return true;
    }

    /** Message type tag used on the wire (JMSMessageType header). */
    public String messageType() {
        return "message";
    }

    /** Copies headers and properties from a (possibly foreign) message into this one. */
    public void copyHeadersFrom(Message m) throws JMSException {
        setJMSMessageID(m.getJMSMessageID());
        setJMSTimestamp(m.getJMSTimestamp());
        setJMSCorrelationID(m.getJMSCorrelationID());
        if (m.getJMSReplyTo() instanceof LavinMQDestination) setJMSReplyTo(m.getJMSReplyTo());
        setJMSDestination(m.getJMSDestination());
        setJMSDeliveryMode(m.getJMSDeliveryMode());
        setJMSRedelivered(m.getJMSRedelivered());
        setJMSType(m.getJMSType());
        setJMSExpiration(m.getJMSExpiration());
        setJMSDeliveryTime(m.getJMSDeliveryTime());
        setJMSPriority(m.getJMSPriority());
        Enumeration<?> names = m.getPropertyNames();
        while (names.hasMoreElements()) {
            String n = (String) names.nextElement();
            properties.put(n, m.getObjectProperty(n));
        }
    }

    protected static MessageFormatException bodyMismatch(Class<?> c, String actual) {
        return new MessageFormatException("message body of type " + actual + " is not assignable to " + c.getName());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{id=" + messageId + ", destination=" + destination + ", properties=" + properties + '}';
    }
}
