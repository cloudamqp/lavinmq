package com.lavinmq.jms.message;

import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.ObjectMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** Holds the body in serialized form; {@link #getObject()} deserializes through the configured filter. */
public final class LavinMQObjectMessage extends LavinMQMessage implements ObjectMessage {
    private byte[] serialized;
    private String filterPattern;

    public LavinMQObjectMessage() {
    }

    public LavinMQObjectMessage(byte[] serialized, String filterPattern) {
        this.serialized = serialized;
        this.filterPattern = filterPattern;
        this.bodyReadOnly = true;
    }

    public void setFilterPattern(String filterPattern) {
        this.filterPattern = filterPattern;
    }

    public byte[] serializedBody() {
        return serialized;
    }

    @Override
    public void setObject(Serializable object) throws JMSException {
        checkBodyWriteable();
        if (object == null) {
            serialized = null;
            return;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                out.writeObject(object);
            }
            serialized = bos.toByteArray();
        } catch (IOException e) {
            MessageFormatException m = new MessageFormatException("object is not serializable: " + e.getMessage());
            m.initCause(e);
            throw m;
        }
    }

    @Override
    public Serializable getObject() throws JMSException {
        if (serialized == null) return null;
        try {
            return ObjectMessageDeserializer.deserialize(serialized, filterPattern);
        } catch (IOException | ClassNotFoundException | RuntimeException e) {
            MessageFormatException m = new MessageFormatException("cannot deserialize object message body: " + e);
            m.initCause(e);
            throw m;
        }
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        serialized = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (serialized == null) return null;
        Serializable o = getObject();
        if (c.isInstance(o)) return (T) o;
        throw bodyMismatch(c, o.getClass().getName());
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        if (serialized == null) return true;
        return c.isInstance(getObject());
    }

    @Override
    public String messageType() {
        return "object";
    }
}
