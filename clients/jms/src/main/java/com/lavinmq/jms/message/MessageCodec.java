package com.lavinmq.jms.message;

import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.destination.DestinationNames;
import com.lavinmq.jms.destination.LavinMQDestination;
import jakarta.jms.BytesMessage;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Maps JMS messages to AMQP properties and bodies and back. */
public final class MessageCodec {
    public static final String H_MESSAGE_TYPE = "JMSMessageType";
    public static final String H_REPLY_TO_TYPE = "JMSReplyToType";
    public static final String H_TIMESTAMP = "JMSTimestamp";
    public static final String H_EXPIRATION = "JMSExpiration";
    public static final String H_DELIVERY_TIME = "JMSDeliveryTime";
    public static final String H_BODY_NULL = "JMSBodyNull";
    public static final String H_DELIVERY_COUNT = "x-delivery-count";
    public static final String H_DELAY = "x-delay";
    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    public static final String JMSX_USER_ID = "JMSXUserID";
    public static final String JMSX_APP_ID = "JMSXAppID";
    public static final String JMSX_GROUP_ID = "JMSXGroupID";
    public static final String JMSX_GROUP_SEQ = "JMSXGroupSeq";
    public static final String JMSX_CONNECTION_ID = "JMSXConnectionID";

    private static final Set<String> OWNED_HEADERS = Set.of(H_MESSAGE_TYPE, H_REPLY_TO_TYPE, H_TIMESTAMP, H_EXPIRATION,
        H_DELIVERY_TIME, H_BODY_NULL, H_DELAY);
    private static final Set<String> NOT_USER_PROPERTIES = Set.of(JMSX_DELIVERY_COUNT, JMSX_USER_ID, JMSX_APP_ID);

    public static final String CT_TEXT = "text/plain; charset=utf-8";
    public static final String CT_BYTES = "application/octet-stream";
    public static final String CT_MAP = "application/x-jms-map";
    public static final String CT_OBJECT = "application/x-java-serialized-object";
    public static final String CT_STREAM = "application/x-jms-stream";

    /** Encoded form of a message, ready to publish. */
    public record Encoded(BasicProperties properties, byte[] body) {
    }

    private final String topicExchange;
    private final String delayedQueueExchange;
    private final String delayedTopicExchange;
    private final String objectFilterPattern;

    public MessageCodec(String topicExchange, String delayedQueueExchange, String delayedTopicExchange, String objectFilterPattern) {
        this.topicExchange = topicExchange;
        this.delayedQueueExchange = delayedQueueExchange;
        this.delayedTopicExchange = delayedTopicExchange;
        this.objectFilterPattern = objectFilterPattern;
    }

    /** Returns the message as one of this provider's classes, copying a foreign implementation. */
    public static LavinMQMessage toLavinMQ(Message m) throws JMSException {
        if (isOwn(m)) return (LavinMQMessage) m;
        LavinMQMessage copy;
        if (m instanceof TextMessage) {
            copy = new LavinMQTextMessage(((TextMessage) m).getText());
        } else if (m instanceof BytesMessage) {
            BytesMessage b = (BytesMessage) m;
            b.reset();
            byte[] data = new byte[(int) b.getBodyLength()];
            b.readBytes(data);
            LavinMQBytesMessage bm = new LavinMQBytesMessage();
            bm.writeBytes(data);
            copy = bm;
        } else if (m instanceof MapMessage) {
            MapMessage mm = (MapMessage) m;
            LavinMQMapMessage out = new LavinMQMapMessage();
            Enumeration<?> names = mm.getMapNames();
            while (names.hasMoreElements()) {
                String n = (String) names.nextElement();
                out.setObject(n, mm.getObject(n));
            }
            copy = out;
        } else if (m instanceof StreamMessage) {
            StreamMessage sm = (StreamMessage) m;
            sm.reset();
            LavinMQStreamMessage out = new LavinMQStreamMessage();
            try {
                while (true) out.writeObject(sm.readObject());
            } catch (jakarta.jms.MessageEOFException eof) {
                // end of stream
            }
            copy = out;
        } else if (m instanceof ObjectMessage) {
            LavinMQObjectMessage om = new LavinMQObjectMessage();
            om.setObject(((ObjectMessage) m).getObject());
            copy = om;
        } else {
            copy = new LavinMQMessage();
        }
        copy.copyHeadersFrom(m);
        return copy;
    }

    private static boolean isOwn(Message m) {
        Class<?> c = m.getClass();
        return c == LavinMQMessage.class || c == LavinMQTextMessage.class || c == LavinMQBytesMessage.class
            || c == LavinMQMapMessage.class || c == LavinMQStreamMessage.class || c == LavinMQObjectMessage.class;
    }

    /** Builds the AMQP properties and body for a message whose JMS headers have already been set by the producer. */
    public Encoded encode(LavinMQMessage m, boolean disableMessageId, boolean disableTimestamp, long ttlMs, long delayMs) throws JMSException {
        Map<String, Object> headers = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : m.propertiesView().entrySet()) {
            if (NOT_USER_PROPERTIES.contains(e.getKey())) continue;
            headers.put(e.getKey(), PropertyCodec.toAmqp(e.getValue()));
        }
        headers.put(H_MESSAGE_TYPE, m.messageType());
        BasicProperties.Builder p = new BasicProperties.Builder();
        p.deliveryMode(m.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ? 2 : 1);
        p.priority(m.getJMSPriority());
        if (m.getJMSCorrelationID() != null) p.correlationId(m.getJMSCorrelationID());
        if (m.getJMSType() != null) p.type(m.getJMSType());
        if (!disableMessageId && m.getJMSMessageID() != null) p.messageId(m.getJMSMessageID());
        if (!disableTimestamp) {
            p.timestamp(m.getJMSTimestamp() / 1000);
            headers.put(H_TIMESTAMP, m.getJMSTimestamp());
        }
        if (ttlMs > 0) {
            p.expiration(Long.toString(ttlMs));
            headers.put(H_EXPIRATION, m.getJMSExpiration());
        }
        if (delayMs > 0) {
            headers.put(H_DELAY, delayMs > Integer.MAX_VALUE ? (Object) delayMs : (Object) (int) delayMs);
            headers.put(H_DELIVERY_TIME, m.getJMSDeliveryTime());
        } else if (m.getJMSDeliveryTime() != 0 && m.getJMSDeliveryTime() != m.getJMSTimestamp()) {
            headers.put(H_DELIVERY_TIME, m.getJMSDeliveryTime());
        }
        if (m.getJMSReplyTo() instanceof LavinMQDestination) {
            LavinMQDestination rt = (LavinMQDestination) m.getJMSReplyTo();
            p.replyTo(rt.name());
            headers.put(H_REPLY_TO_TYPE, DestinationNames.replyToType(rt));
        }
        byte[] body;
        if (m instanceof LavinMQTextMessage) {
            String text = ((LavinMQTextMessage) m).getText();
            p.contentType(CT_TEXT);
            if (text == null) {
                headers.put(H_BODY_NULL, true);
                body = new byte[0];
            } else {
                body = text.getBytes(StandardCharsets.UTF_8);
            }
        } else if (m instanceof LavinMQBytesMessage) {
            p.contentType(CT_BYTES);
            body = ((LavinMQBytesMessage) m).bytes();
        } else if (m instanceof LavinMQMapMessage) {
            p.contentType(CT_MAP);
            body = TypedValueCodec.encodeMap(((LavinMQMapMessage) m).mapView());
        } else if (m instanceof LavinMQStreamMessage) {
            p.contentType(CT_STREAM);
            body = TypedValueCodec.encodeStream(((LavinMQStreamMessage) m).valuesView());
        } else if (m instanceof LavinMQObjectMessage) {
            p.contentType(CT_OBJECT);
            byte[] ser = ((LavinMQObjectMessage) m).serializedBody();
            if (ser == null) {
                headers.put(H_BODY_NULL, true);
                body = new byte[0];
            } else {
                body = ser;
            }
        } else {
            body = new byte[0];
        }
        p.headers(headers);
        return new Encoded(p.build(), body);
    }

    /** Rebuilds a JMS message from a delivery. */
    public LavinMQMessage decode(BasicProperties props, byte[] body, String exchange, String routingKey, boolean redelivered) throws JMSException {
        Map<String, Object> headers = props.headers() == null ? Map.of() : props.headers();
        Object typeHeader = headers.get(H_MESSAGE_TYPE);
        String type = typeHeader instanceof String ? (String) typeHeader : guessType(props.contentType());
        boolean bodyNull = Boolean.TRUE.equals(headers.get(H_BODY_NULL));
        LavinMQMessage m;
        switch (type) {
            case "text":
                m = new LavinMQTextMessage(bodyNull ? null : new String(body, StandardCharsets.UTF_8));
                break;
            case "map":
                m = new LavinMQMapMessage(TypedValueCodec.decodeMap(body));
                break;
            case "stream":
                m = new LavinMQStreamMessage(TypedValueCodec.decodeStream(body));
                break;
            case "object":
                m = new LavinMQObjectMessage(bodyNull ? null : body, objectFilterPattern);
                break;
            case "message":
                m = new LavinMQMessage();
                break;
            default:
                m = new LavinMQBytesMessage(body);
                break;
        }
        m.setBodyReadOnly(true);

        if (props.messageId() != null) {
            String id = props.messageId();
            m.setJMSMessageID(id.startsWith("ID:") ? id : "ID:" + id);
        }
        long ts = 0;
        if (headers.get(H_TIMESTAMP) instanceof Number) ts = ((Number) headers.get(H_TIMESTAMP)).longValue();
        else if (props.timestamp() != null) ts = props.timestamp() * 1000;
        m.setJMSTimestamp(ts);
        m.setJMSCorrelationID(props.correlationId());
        m.setJMSDeliveryMode(props.deliveryMode() != null && props.deliveryMode() == 1 ? DeliveryMode.NON_PERSISTENT : DeliveryMode.PERSISTENT);
        int prio = props.priority() == null ? Message.DEFAULT_PRIORITY : props.priority();
        m.setJMSPriority(Math.max(0, Math.min(9, prio)));
        m.setJMSType(props.type());
        long expiration = 0;
        if (headers.get(H_EXPIRATION) instanceof Number) {
            expiration = ((Number) headers.get(H_EXPIRATION)).longValue();
        } else if (props.expiration() != null && ts != 0) {
            try {
                expiration = ts + Long.parseLong(props.expiration());
            } catch (NumberFormatException ignored) {
                // leave at 0
            }
        }
        m.setJMSExpiration(expiration);
        m.setJMSDeliveryTime(headers.get(H_DELIVERY_TIME) instanceof Number ? ((Number) headers.get(H_DELIVERY_TIME)).longValue() : ts);
        m.setJMSDestination(DestinationNames.fromDelivery(exchange, routingKey, topicExchange, delayedQueueExchange, delayedTopicExchange));
        if (props.replyTo() != null) {
            Object rtt = headers.get(H_REPLY_TO_TYPE);
            m.setJMSReplyTo(DestinationNames.fromReplyTo(props.replyTo(), rtt instanceof String ? (String) rtt : null));
        }
        int deliveryCount = 1;
        if (headers.get(H_DELIVERY_COUNT) instanceof Number) deliveryCount = ((Number) headers.get(H_DELIVERY_COUNT)).intValue() + 1;
        else if (redelivered) deliveryCount = 2;
        m.setJMSRedelivered(redelivered || deliveryCount > 1);

        for (Map.Entry<String, Object> e : headers.entrySet()) {
            if (OWNED_HEADERS.contains(e.getKey())) continue;
            m.setPropertyInternal(e.getKey(), PropertyCodec.fromAmqp(e.getValue()));
        }
        m.setPropertyInternal(JMSX_DELIVERY_COUNT, deliveryCount);
        if (props.userId() != null) m.setPropertyInternal(JMSX_USER_ID, props.userId());
        if (props.appId() != null) m.setPropertyInternal(JMSX_APP_ID, props.appId());
        m.setPropertiesReadOnly(true);
        return m;
    }

    static String guessType(String contentType) {
        if (contentType == null) return "bytes";
        String ct = contentType.toLowerCase(Locale.ROOT);
        if (ct.startsWith("text/") || ct.startsWith("application/json") || ct.startsWith("application/xml")) return "text";
        if (ct.startsWith(CT_OBJECT)) return "object";
        if (ct.startsWith(CT_MAP)) return "map";
        if (ct.startsWith(CT_STREAM)) return "stream";
        return "bytes";
    }
}
