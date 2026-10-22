package com.lavinmq.jms.message;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.jms.MessageFormatException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypedValueCodecTest {
    @Test
    void mapRoundTrip() throws Exception {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("bool", true);
        m.put("byte", (byte) 1);
        m.put("short", (short) 2);
        m.put("char", 'c');
        m.put("int", 4);
        m.put("long", 5L);
        m.put("float", 6.5f);
        m.put("double", 7.25);
        m.put("string", "héllo");
        m.put("bytes", new byte[] {9, 10});
        m.put("null", null);
        Map<String, Object> back = TypedValueCodec.decodeMap(TypedValueCodec.encodeMap(m));
        assertEquals(List.copyOf(m.keySet()), List.copyOf(back.keySet()));
        assertEquals(true, back.get("bool"));
        assertEquals((byte) 1, back.get("byte"));
        assertEquals((short) 2, back.get("short"));
        assertEquals('c', back.get("char"));
        assertEquals(4, back.get("int"));
        assertEquals(5L, back.get("long"));
        assertEquals(6.5f, back.get("float"));
        assertEquals(7.25, back.get("double"));
        assertEquals("héllo", back.get("string"));
        assertArrayEquals(new byte[] {9, 10}, (byte[]) back.get("bytes"));
        assertNull(back.get("null"));
        assertEquals(Map.of(), TypedValueCodec.decodeMap(TypedValueCodec.encodeMap(Map.of())));
    }

    @Test
    void streamRoundTrip() throws Exception {
        List<Object> values = Arrays.asList(1, "two", null, new byte[] {3}, 'x', 4.0);
        List<Object> back = TypedValueCodec.decodeStream(TypedValueCodec.encodeStream(values));
        assertEquals(6, back.size());
        assertEquals(1, back.get(0));
        assertEquals("two", back.get(1));
        assertNull(back.get(2));
        assertArrayEquals(new byte[] {3}, (byte[]) back.get(3));
        assertEquals('x', back.get(4));
        assertEquals(4.0, back.get(5));
    }

    @Test
    void malformed() {
        assertThrows(MessageFormatException.class, () -> TypedValueCodec.decodeMap(new byte[] {1, 2, 3}));
        assertThrows(MessageFormatException.class, () -> TypedValueCodec.decodeMap(TypedValueCodec.encodeStream(List.of())));
        assertThrows(MessageFormatException.class, () -> TypedValueCodec.decodeStream(new byte[] {'L', 'M', 'Q', 'S', 9}));
        assertThrows(MessageFormatException.class, () -> TypedValueCodec.decodeStream(new byte[] {'L', 'M', 'Q', 'S', 1, 99}));
    }
}
