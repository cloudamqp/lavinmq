package com.lavinmq.jms.context;

import com.lavinmq.jms.JMSExceptions;
import com.lavinmq.jms.LavinMQMessageProducer;
import com.lavinmq.jms.message.LavinMQBytesMessage;
import com.lavinmq.jms.message.LavinMQMapMessage;
import com.lavinmq.jms.message.LavinMQMessage;
import com.lavinmq.jms.message.LavinMQObjectMessage;
import com.lavinmq.jms.message.LavinMQTextMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Simplified-API producer: a mutable set of send options over an anonymous {@link LavinMQMessageProducer}. */
public final class LavinMQJMSProducer implements JMSProducer {
    private final LavinMQJMSContext context;
    private LavinMQMessageProducer producer;
    private final Map<String, Object> properties = new LinkedHashMap<>();
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive;
    private long deliveryDelay;
    private CompletionListener completionListener;
    private String correlationId;
    private String type;
    private Destination replyTo;

    LavinMQJMSProducer(LavinMQJMSContext context) {
        this.context = context;
    }

    private synchronized LavinMQMessageProducer producer() {
        if (producer == null) {
            producer = LavinMQJMSContext.unchecked(() -> (LavinMQMessageProducer) context.session().createProducer(null));
        }
        return producer;
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        if (message == null) throw new MessageFormatRuntimeException("message is null");
        try {
            LavinMQMessage m = com.lavinmq.jms.message.MessageCodec.toLavinMQ(message);
            for (Map.Entry<String, Object> e : properties.entrySet()) m.setObjectProperty(e.getKey(), e.getValue());
            if (correlationId != null) m.setJMSCorrelationID(correlationId);
            if (type != null) m.setJMSType(type);
            if (replyTo != null) m.setJMSReplyTo(replyTo);
            LavinMQMessageProducer p = producer();
            p.setDisableMessageID(disableMessageID);
            p.setDisableMessageTimestamp(disableMessageTimestamp);
            p.setDeliveryDelay(deliveryDelay);
            if (completionListener != null) p.send(destination, m, deliveryMode, priority, timeToLive, completionListener);
            else p.send(destination, m, deliveryMode, priority, timeToLive);
            if (m != message) copyBackHeaders(m, message);
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
        return this;
    }

    private static void copyBackHeaders(LavinMQMessage from, Message to) throws JMSException {
        to.setJMSMessageID(from.getJMSMessageID());
        to.setJMSTimestamp(from.getJMSTimestamp());
        to.setJMSExpiration(from.getJMSExpiration());
        to.setJMSDeliveryTime(from.getJMSDeliveryTime());
        to.setJMSDestination(from.getJMSDestination());
        to.setJMSDeliveryMode(from.getJMSDeliveryMode());
        to.setJMSPriority(from.getJMSPriority());
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        return send(destination, new LavinMQTextMessage(body));
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        LavinMQMapMessage m = new LavinMQMapMessage();
        if (body != null) {
            try {
                for (Map.Entry<String, Object> e : body.entrySet()) m.setObject(e.getKey(), e.getValue());
            } catch (JMSException e) {
                throw JMSExceptions.toRuntime(e);
            }
        }
        return send(destination, m);
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        LavinMQBytesMessage m = new LavinMQBytesMessage();
        if (body != null) {
            try {
                m.writeBytes(body);
            } catch (JMSException e) {
                throw JMSExceptions.toRuntime(e);
            }
        }
        return send(destination, m);
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        LavinMQObjectMessage m = new LavinMQObjectMessage();
        try {
            m.setObject(body);
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
        return send(destination, m);
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        disableMessageID = value;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        disableMessageTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSRuntimeException("invalid delivery mode " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        if (priority < 0 || priority > 9) throw new JMSRuntimeException("priority out of range: " + priority);
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        if (timeToLive < 0) throw new JMSRuntimeException("time to live must not be negative");
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        if (deliveryDelay < 0) throw new JMSRuntimeException("delivery delay must not be negative");
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    private JMSProducer put(String name, Object value) {
        try {
            LavinMQMessage probe = new LavinMQMessage();
            probe.setObjectProperty(name, value); // validates name and type
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
        properties.put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        return put(name, value);
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        return put(name, value);
    }

    @Override
    public JMSProducer clearProperties() {
        properties.clear();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties.containsKey(name);
    }

    private LavinMQMessage view() {
        LavinMQMessage m = new LavinMQMessage();
        for (Map.Entry<String, Object> e : properties.entrySet()) m.setPropertyInternal(e.getKey(), e.getValue());
        return m;
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getBooleanProperty(name));
    }

    @Override
    public byte getByteProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getByteProperty(name));
    }

    @Override
    public short getShortProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getShortProperty(name));
    }

    @Override
    public int getIntProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getIntProperty(name));
    }

    @Override
    public long getLongProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getLongProperty(name));
    }

    @Override
    public float getFloatProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getFloatProperty(name));
    }

    @Override
    public double getDoubleProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getDoubleProperty(name));
    }

    @Override
    public String getStringProperty(String name) {
        return LavinMQJMSContext.unchecked(() -> view().getStringProperty(name));
    }

    @Override
    public Object getObjectProperty(String name) {
        return properties.get(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return Set.copyOf(properties.keySet());
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        throw new UnsupportedOperationException("byte correlation ids are not supported");
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        throw new UnsupportedOperationException("byte correlation ids are not supported");
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        this.correlationId = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return correlationId;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }
}
