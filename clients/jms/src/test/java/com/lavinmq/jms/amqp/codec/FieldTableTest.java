package com.lavinmq.jms.amqp.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.ProtocolException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldTableTest {
    private static byte[] encode(Map<String, Object> t) {
        AmqpWriter w = new AmqpWriter();
        FieldTable.write(t, w);
        return w.toByteArray();
    }

    private static Map<String, Object> decode(byte[] b) throws ProtocolException {
        return FieldTable.read(new AmqpReader(b));
    }

    @Test
    void goldenBooleanTable() throws Exception {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("a", true);
        assertEquals("00 00 00 04 01 61 74 01", Hex.hex(encode(t)));
        assertEquals(t, decode(Hex.bytes("00 00 00 04 01 61 74 01")));
        assertEquals("00 00 00 00", Hex.hex(encode(Map.of())));
        assertEquals("00 00 00 00", Hex.hex(encode(null)));
    }

    @Test
    void everyTypeRoundTrips() throws Exception {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("bool", false);
        t.put("byte", (byte) -5);
        t.put("short", (short) -300);
        t.put("int", -70000);
        t.put("long", 1L << 40);
        t.put("float", 1.5f);
        t.put("double", Math.PI);
        t.put("string", "héllo wörld");
        t.put("bytes", new byte[] {1, 2, 3});
        t.put("null", null);
        t.put("ts", Instant.ofEpochSecond(1700000000L));
        t.put("dec", new BigDecimal("3.14"));
        t.put("list", Arrays.asList(1, "two", null, Arrays.asList(3L)));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("x", "y");
        t.put("map", nested);

        Map<String, Object> back = decode(encode(t));
        assertEquals(false, back.get("bool"));
        assertEquals((byte) -5, back.get("byte"));
        assertEquals((short) -300, back.get("short"));
        assertEquals(-70000, back.get("int"));
        assertEquals(1L << 40, back.get("long"));
        assertEquals(1.5f, back.get("float"));
        assertEquals(Math.PI, back.get("double"));
        assertEquals("héllo wörld", back.get("string"));
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) back.get("bytes"));
        assertTrue(back.containsKey("null"));
        assertNull(back.get("null"));
        assertEquals(Instant.ofEpochSecond(1700000000L), back.get("ts"));
        assertEquals(new BigDecimal("3.14"), back.get("dec"));
        assertEquals(Arrays.asList(1, "two", null, List.of(3L)), back.get("list"));
        assertEquals(nested, back.get("map"));
        assertEquals(List.copyOf(t.keySet()), List.copyOf(back.keySet()));
    }

    @Test
    void decimalEncoding() throws Exception {
        AmqpWriter w = new AmqpWriter();
        FieldTable.writeValue(new BigDecimal("3.14"), w);
        assertEquals("44 02 00 00 01 3A", Hex.hex(w.toByteArray()));
        assertEquals(new BigDecimal("3.14"), FieldTable.readValue(new AmqpReader(Hex.bytes("44 02 00 00 01 3A"))));
        assertThrows(IllegalArgumentException.class, () -> FieldTable.writeValue(new BigDecimal("12345678901234"), new AmqpWriter()));
    }

    @Test
    void readOnlyTags() throws Exception {
        assertEquals((short) 7, FieldTable.readValue(new AmqpReader(Hex.bytes("42 07"))));
        assertEquals((short) -1, FieldTable.readValue(new AmqpReader(Hex.bytes("55 FF FF"))));
        assertEquals(65535, FieldTable.readValue(new AmqpReader(Hex.bytes("75 FF FF"))));
        assertEquals(4294967295L, FieldTable.readValue(new AmqpReader(Hex.bytes("69 FF FF FF FF"))));
        assertEquals(-1L, FieldTable.readValue(new AmqpReader(Hex.bytes("4C FF FF FF FF FF FF FF FF"))));
    }

    @Test
    void errors() {
        assertThrows(ProtocolException.class, () -> FieldTable.readValue(new AmqpReader(Hex.bytes("5A 00"))));
        // declared length longer than data
        assertThrows(ProtocolException.class, () -> decode(Hex.bytes("00 00 00 10 01 61 74 01")));
        // value overruns declared length
        assertThrows(ProtocolException.class, () -> decode(Hex.bytes("00 00 00 03 01 61 74 01")));
        Map<String, Object> longKey = new LinkedHashMap<>();
        longKey.put("k".repeat(129), 1);
        assertThrows(IllegalArgumentException.class, () -> encode(longKey));
        Map<String, Object> badType = new LinkedHashMap<>();
        badType.put("o", new Object());
        assertThrows(IllegalArgumentException.class, () -> encode(badType));
    }
}
