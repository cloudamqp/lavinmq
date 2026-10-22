package com.lavinmq.jms.message;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.destination.LavinMQQueue;
import com.lavinmq.jms.destination.LavinMQTemporaryQueue;
import com.lavinmq.jms.destination.LavinMQTopic;
import jakarta.jms.DeliveryMode;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageCodecTest {
    private final MessageCodec codec = new MessageCodec("jms.topic", "jms.delayed", "jms.delayed.topic", ObjectMessageDeserializer.DEFAULT_FILTER);

    @Test
    void textMessageRoundTripWithAllHeaders() throws Exception {
        LavinMQTextMessage m = new LavinMQTextMessage("hello");
        m.setJMSMessageID("ID:abc");
        m.setJMSTimestamp(1700000000123L);
        m.setJMSCorrelationID("corr");
        m.setJMSReplyTo(new LavinMQTemporaryQueue("amq.gen-1", "c", null));
        m.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        m.setJMSType("t");
        m.setJMSExpiration(1700000060123L);
        m.setJMSDeliveryTime(1700000005123L);
        m.setJMSPriority(7);
        m.setStringProperty("user", "u");
        m.setIntProperty("n", 3);
        m.setDoubleProperty("d", 2.5);
        m.setPropertyInternal("JMSXGroupID", "g1");
        MessageCodec.Encoded e = codec.encode(m, false, false, 60000, 5000);
        BasicProperties p = e.properties();
        assertEquals("ID:abc", p.messageId());
        assertEquals(1700000000L, p.timestamp());
        assertEquals("corr", p.correlationId());
        assertEquals("amq.gen-1", p.replyTo());
        assertEquals(1, p.deliveryMode());
        assertEquals(7, p.priority());
        assertEquals("t", p.type());
        assertEquals("60000", p.expiration());
        assertEquals(MessageCodec.CT_TEXT, p.contentType());
        Map<String, Object> h = p.headers();
        assertEquals("text", h.get("JMSMessageType"));
        assertEquals("temp-queue", h.get("JMSReplyToType"));
        assertEquals(1700000000123L, h.get("JMSTimestamp"));
        assertEquals(1700000060123L, h.get("JMSExpiration"));
        assertEquals(1700000005123L, h.get("JMSDeliveryTime"));
        assertEquals(5000, h.get("x-delay"));
        assertEquals("u", h.get("user"));
        assertEquals(3, h.get("n"));
        assertEquals("g1", h.get("JMSXGroupID"));
        assertEquals("hello", new String(e.body(), StandardCharsets.UTF_8));

        // the delayed exchange strips x-delay; simulate that
        Map<String, Object> delivered = new java.util.LinkedHashMap<>(h);
        delivered.remove("x-delay");
        delivered.put("x-delivery-count", 2);
        LavinMQMessage back = codec.decode(p.builder().headers(delivered).build(), e.body(), "", "orders", true);
        assertTrue(back instanceof LavinMQTextMessage);
        assertEquals("hello", ((LavinMQTextMessage) back).getText());
        assertEquals("ID:abc", back.getJMSMessageID());
        assertEquals(1700000000123L, back.getJMSTimestamp());
        assertEquals("corr", back.getJMSCorrelationID());
        assertTrue(back.getJMSReplyTo() instanceof LavinMQTemporaryQueue);
        assertEquals("amq.gen-1", ((LavinMQTemporaryQueue) back.getJMSReplyTo()).getQueueName());
        assertEquals(DeliveryMode.NON_PERSISTENT, back.getJMSDeliveryMode());
        assertEquals("t", back.getJMSType());
        assertEquals(1700000060123L, back.getJMSExpiration());
        assertEquals(1700000005123L, back.getJMSDeliveryTime());
        assertEquals(7, back.getJMSPriority());
        assertEquals(new LavinMQQueue("orders"), back.getJMSDestination());
        assertTrue(back.getJMSRedelivered());
        assertEquals(3, back.getIntProperty("JMSXDeliveryCount"));
        assertEquals("u", back.getStringProperty("user"));
        assertEquals(3, back.getIntProperty("n"));
        assertEquals(2.5, back.getDoubleProperty("d"));
        assertEquals("g1", back.getStringProperty("JMSXGroupID"));
        assertEquals(2, back.getIntProperty("x-delivery-count"));
        assertFalse(Collections.list(back.getPropertyNames()).contains("JMSMessageType"));
        assertFalse(Collections.list(back.getPropertyNames()).contains("JMSTimestamp"));
        assertTrue(Collections.list(back.getPropertyNames()).contains("JMSXDeliveryCount"));
    }

    @Test
    void nullTextBodyAndDisabledHeaders() throws Exception {
        LavinMQTextMessage m = new LavinMQTextMessage(null);
        m.setJMSMessageID("ID:x");
        m.setJMSTimestamp(12345);
        MessageCodec.Encoded e = codec.encode(m, true, true, 0, 0);
        assertNull(e.properties().messageId());
        assertNull(e.properties().timestamp());
        assertNull(e.properties().expiration());
        assertEquals(true, e.properties().headers().get("JMSBodyNull"));
        LavinMQMessage back = codec.decode(e.properties(), e.body(), "jms.topic", "news", false);
        assertNull(((LavinMQTextMessage) back).getText());
        assertNull(back.getJMSMessageID());
        assertEquals(0, back.getJMSTimestamp());
        assertEquals(0, back.getJMSExpiration());
        assertEquals(new LavinMQTopic("news"), back.getJMSDestination());
        assertEquals(1, back.getIntProperty("JMSXDeliveryCount"));
        assertFalse(back.getJMSRedelivered());
    }

    @Test
    void foreignAmqpMessages() throws Exception {
        BasicProperties text = new BasicProperties.Builder().contentType("application/json").messageId("m-1").timestamp(1700000000L).expiration("1000").build();
        LavinMQMessage t = codec.decode(text, "{}".getBytes(), "", "q", false);
        assertTrue(t instanceof LavinMQTextMessage);
        assertEquals("{}", ((LavinMQTextMessage) t).getText());
        assertEquals("ID:m-1", t.getJMSMessageID());
        assertEquals(1700000000000L, t.getJMSTimestamp());
        assertEquals(1700000001000L, t.getJMSExpiration());
        assertEquals(DeliveryMode.PERSISTENT, t.getJMSDeliveryMode());
        assertEquals(4, t.getJMSPriority());

        LavinMQMessage b = codec.decode(BasicProperties.EMPTY, new byte[] {1}, "", "q", false);
        assertTrue(b instanceof LavinMQBytesMessage);
        assertArrayEquals(new byte[] {1}, b.getBody(byte[].class));

        BasicProperties withReply = new BasicProperties.Builder().replyTo("rpc.replies").priority(12).build();
        LavinMQMessage r = codec.decode(withReply, new byte[0], "amq.topic", "k", false);
        assertEquals(new LavinMQQueue("rpc.replies"), r.getJMSReplyTo());
        assertEquals(9, r.getJMSPriority());
        assertEquals("amq.topic", ((LavinMQTopic) r.getJMSDestination()).exchange());

        BasicProperties odd = new BasicProperties.Builder().headers(Map.of("ts", java.time.Instant.ofEpochSecond(5), "dec", new java.math.BigDecimal("1.5"))).build();
        LavinMQMessage o = codec.decode(odd, new byte[0], "", "q", false);
        assertEquals(5000L, o.getLongProperty("ts"));
        assertEquals("1.5", o.getStringProperty("dec"));
    }

    @Test
    void mapStreamObjectBytesBodies() throws Exception {
        LavinMQMapMessage map = new LavinMQMapMessage();
        map.setInt("a", 1);
        LavinMQMessage m1 = roundTrip(map);
        assertEquals(1, ((LavinMQMapMessage) m1).getInt("a"));

        LavinMQStreamMessage stream = new LavinMQStreamMessage();
        stream.writeString("s");
        LavinMQStreamMessage m2 = (LavinMQStreamMessage) roundTrip(stream);
        assertEquals("s", m2.readString());

        LavinMQObjectMessage obj = new LavinMQObjectMessage();
        obj.setObject(42);
        assertEquals(42, ((LavinMQObjectMessage) roundTrip(obj)).getObject());
        LavinMQObjectMessage nullObj = new LavinMQObjectMessage();
        assertNull(((LavinMQObjectMessage) roundTrip(nullObj)).getObject());

        LavinMQBytesMessage bytes = new LavinMQBytesMessage();
        bytes.writeLong(9L);
        LavinMQBytesMessage m4 = (LavinMQBytesMessage) roundTrip(bytes);
        assertEquals(9L, m4.readLong());

        LavinMQMessage plain = roundTrip(new LavinMQMessage());
        assertEquals(LavinMQMessage.class, plain.getClass());
    }

    private LavinMQMessage roundTrip(LavinMQMessage m) throws Exception {
        MessageCodec.Encoded e = codec.encode(m, false, false, 0, 0);
        return codec.decode(e.properties(), e.body(), "", "q", false);
    }

    @Test
    void foreignJmsImplementationIsCopied() throws Exception {
        jakarta.jms.TextMessage foreign = new ForeignText();
        LavinMQMessage copy = MessageCodec.toLavinMQ(foreign);
        assertTrue(copy instanceof LavinMQTextMessage);
        assertEquals("foreign", ((LavinMQTextMessage) copy).getText());
        assertEquals("v", copy.getStringProperty("k"));
        assertEquals(9, copy.getJMSPriority());
    }

    /** Minimal foreign TextMessage built on our base class with a different body holder. */
    static final class ForeignText extends LavinMQMessage implements jakarta.jms.TextMessage {
        ForeignText() throws Exception {
            setStringProperty("k", "v");
            setJMSPriority(9);
        }

        @Override
        public void setText(String string) {
        }

        @Override
        public String getText() {
            return "foreign";
        }
    }
}
