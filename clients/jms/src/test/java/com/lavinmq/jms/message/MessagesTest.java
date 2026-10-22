package com.lavinmq.jms.message;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessagesTest {
    @Test
    void propertyConversionTable() throws Exception {
        LavinMQMessage m = new LavinMQMessage();
        m.setIntProperty("i", 42);
        m.setStringProperty("s", "17");
        m.setBooleanProperty("b", true);
        m.setFloatProperty("f", 1.5f);
        m.setByteProperty("by", (byte) 3);
        assertEquals(42L, m.getLongProperty("i"));
        assertEquals("42", m.getStringProperty("i"));
        assertEquals(17, m.getIntProperty("s"));
        assertEquals((short) 17, m.getShortProperty("s"));
        assertEquals(17.0, m.getDoubleProperty("s"));
        assertTrue(m.getBooleanProperty("b"));
        assertEquals("true", m.getStringProperty("b"));
        assertEquals(1.5, m.getDoubleProperty("f"));
        assertEquals(3, m.getIntProperty("by"));
        assertThrows(MessageFormatException.class, () -> m.getIntProperty("b"));
        assertThrows(MessageFormatException.class, () -> m.getFloatProperty("i"));
        assertThrows(MessageFormatException.class, () -> m.getByteProperty("i"));
        assertThrows(NumberFormatException.class, () -> m.getIntProperty("missing"));
        assertFalse(m.getBooleanProperty("missing"));
        assertNull(m.getStringProperty("missing"));
        assertThrows(IllegalArgumentException.class, () -> m.setStringProperty("NULL", "x"));
        assertThrows(IllegalArgumentException.class, () -> m.setStringProperty("1abc", "x"));
        assertThrows(IllegalArgumentException.class, () -> m.setStringProperty("", "x"));
        assertThrows(MessageFormatException.class, () -> m.setObjectProperty("o", new Object()));
        m.setPropertiesReadOnly(true);
        assertThrows(MessageNotWriteableException.class, () -> m.setIntProperty("x", 1));
        m.clearProperties();
        m.setIntProperty("x", 1);
        assertEquals(1, m.getIntProperty("x"));
        assertEquals(Collections.list(m.getPropertyNames()), new ArrayList<>(java.util.List.of("x")));
    }

    @Test
    void headerValidation() throws Exception {
        LavinMQMessage m = new LavinMQMessage();
        assertThrows(jakarta.jms.JMSException.class, () -> m.setJMSPriority(10));
        assertThrows(jakarta.jms.JMSException.class, () -> m.setJMSDeliveryMode(3));
        assertThrows(UnsupportedOperationException.class, () -> m.setJMSCorrelationIDAsBytes(new byte[0]));
        assertNull(m.getBody(String.class));
        assertTrue(m.isBodyAssignableTo(String.class));
    }

    @Test
    void bytesMessageModes() throws Exception {
        LavinMQBytesMessage b = new LavinMQBytesMessage();
        assertThrows(MessageNotReadableException.class, b::readInt);
        b.writeInt(7);
        b.writeUTF("x");
        b.writeObject(new byte[] {1, 2});
        assertThrows(MessageFormatException.class, () -> b.writeObject(new Object()));
        b.reset();
        assertEquals(4 + 3 + 2, b.getBodyLength());
        assertEquals(7, b.readInt());
        assertEquals("x", b.readUTF());
        byte[] rest = new byte[10];
        assertEquals(2, b.readBytes(rest));
        assertEquals(-1, b.readBytes(rest));
        assertThrows(MessageEOFException.class, b::readByte);
        assertThrows(MessageNotWriteableException.class, () -> b.writeInt(1));
        assertArrayEquals(b.bytes(), b.getBody(byte[].class));
        assertEquals(7, b.readInt());
        b.clearBody();
        b.writeByte((byte) 1);
        assertEquals(1, b.bytes().length);
        LavinMQBytesMessage received = new LavinMQBytesMessage(new byte[] {5});
        assertEquals(5, received.readByte());
        assertThrows(MessageNotWriteableException.class, () -> received.writeByte((byte) 1));
    }

    @Test
    void mapMessage() throws Exception {
        LavinMQMapMessage m = new LavinMQMapMessage();
        m.setInt("i", 5);
        m.setChar("c", 'z');
        m.setBytes("b", new byte[] {1, 2, 3}, 1, 2);
        m.setObject("s", "str");
        assertEquals("5", m.getString("i"));
        assertEquals(5L, m.getLong("i"));
        assertEquals('z', m.getChar("c"));
        assertEquals("z", m.getString("c"));
        assertArrayEquals(new byte[] {2, 3}, m.getBytes("b"));
        assertThrows(MessageFormatException.class, () -> m.getString("b"));
        assertThrows(MessageFormatException.class, () -> m.getChar("i"));
        assertThrows(NullPointerException.class, () -> m.getChar("missing"));
        assertTrue(m.itemExists("i"));
        assertEquals(4, Collections.list(m.getMapNames()).size());
        Map<?, ?> body = m.getBody(Map.class);
        assertEquals(5, body.get("i"));
        assertThrows(MessageFormatException.class, () -> m.getBody(String.class));
        m.setBodyReadOnly(true);
        assertThrows(MessageNotWriteableException.class, () -> m.setInt("x", 1));
    }

    @Test
    void streamMessage() throws Exception {
        LavinMQStreamMessage s = new LavinMQStreamMessage();
        s.writeInt(1);
        s.writeString("2");
        s.writeBytes(new byte[] {1, 2, 3, 4, 5});
        s.writeObject(null);
        s.writeBoolean(true);
        assertThrows(MessageNotReadableException.class, s::readInt);
        s.reset();
        assertEquals(1L, s.readLong());
        assertThrows(MessageFormatException.class, s::readChar); // the pointer does not move on a failed read
        assertEquals(2, s.readInt());
        byte[] buf = new byte[2];
        assertEquals(2, s.readBytes(buf));
        assertArrayEquals(new byte[] {1, 2}, buf);
        assertEquals(2, s.readBytes(buf));
        assertEquals(1, s.readBytes(buf)); // less than the buffer: the field is finished
        assertEquals(-1, s.readBytes(buf)); // null field
        assertTrue(s.readBoolean());
        assertThrows(MessageEOFException.class, s::readObject);
        s.reset();
        assertEquals(1, s.readObject());
        assertThrows(MessageFormatException.class, () -> s.getBody(Object.class));
        assertFalse(s.isBodyAssignableTo(Object.class));
    }

    @Test
    void textAndObjectMessages() throws Exception {
        LavinMQTextMessage t = new LavinMQTextMessage("hi");
        assertEquals("hi", t.getBody(String.class));
        assertEquals("hi", t.getBody(CharSequence.class));
        assertThrows(MessageFormatException.class, () -> t.getBody(Integer.class));
        t.setBodyReadOnly(true);
        assertThrows(MessageNotWriteableException.class, () -> t.setText("x"));
        t.clearBody();
        assertNull(t.getText());
        t.setText("again");
        assertEquals("again", t.getText());

        LavinMQObjectMessage o = new LavinMQObjectMessage();
        o.setObject(new java.util.ArrayList<>(java.util.List.of("a")));
        assertEquals(java.util.List.of("a"), o.getObject());
        assertEquals(java.util.List.of("a"), o.getBody(java.util.List.class));
        assertTrue(o.isBodyAssignableTo(java.util.List.class));
        assertFalse(o.isBodyAssignableTo(String.class));
        o.setObject(null);
        assertNull(o.getObject());
        assertThrows(MessageFormatException.class, () -> o.setObject(new NotSerializableHolder()));
    }

    @Test
    void objectMessageFilterRejectsUnlistedClasses() throws Exception {
        LavinMQObjectMessage o = new LavinMQObjectMessage();
        o.setObject(new CustomPayload("x"));
        LavinMQObjectMessage received = new LavinMQObjectMessage(o.serializedBody(), ObjectMessageDeserializer.DEFAULT_FILTER);
        assertThrows(MessageFormatException.class, received::getObject);
        LavinMQObjectMessage allowed = new LavinMQObjectMessage(o.serializedBody(), "com.lavinmq.jms.message.*;java.base/*;!*");
        assertEquals("x", ((CustomPayload) allowed.getObject()).value);
    }

    static final class CustomPayload implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        final String value;

        CustomPayload(String value) {
            this.value = value;
        }
    }

    static final class NotSerializableHolder implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        @SuppressWarnings("unused")
        private final Object o = new Object();
    }
}
