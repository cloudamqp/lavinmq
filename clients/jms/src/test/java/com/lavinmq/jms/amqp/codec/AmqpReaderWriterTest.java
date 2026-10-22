package com.lavinmq.jms.amqp.codec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.ProtocolException;
import org.junit.jupiter.api.Test;

class AmqpReaderWriterTest {
    @Test
    void integerBoundaries() throws Exception {
        AmqpWriter w = new AmqpWriter();
        w.octet(0).octet(255).short_(0).short_(0xFFFF).long_(0).long_(0xFFFFFFFFL).longlong(Long.MIN_VALUE).longlong(-1L);
        AmqpReader r = new AmqpReader(w.toByteArray());
        assertEquals(0, r.octet());
        assertEquals(255, r.octet());
        assertEquals(0, r.short_());
        assertEquals(0xFFFF, r.short_());
        assertEquals(0L, r.long_());
        assertEquals(0xFFFFFFFFL, r.long_());
        assertEquals(Long.MIN_VALUE, r.longlong());
        assertEquals(-1L, r.longlong());
        assertEquals(0, r.remaining());
        assertThrows(IllegalArgumentException.class, () -> new AmqpWriter().short_(0x10000));
        assertThrows(IllegalArgumentException.class, () -> new AmqpWriter().long_(-1));
    }

    @Test
    void shortStringLimits() throws Exception {
        String max = "a".repeat(255);
        AmqpWriter w = new AmqpWriter().shortstr(max);
        assertEquals(256, w.size());
        assertEquals(max, new AmqpReader(w.toByteArray()).shortstr());
        assertThrows(IllegalArgumentException.class, () -> new AmqpWriter().shortstr("a".repeat(256)));
        // multi-byte UTF-8 is counted in bytes, not chars
        assertThrows(IllegalArgumentException.class, () -> new AmqpWriter().shortstr("é".repeat(128)));
        AmqpWriter w2 = new AmqpWriter().shortstr("héllo");
        assertEquals(1 + 6, w2.size());
        assertEquals("héllo", new AmqpReader(w2.toByteArray()).shortstr());
    }

    @Test
    void longString() throws Exception {
        byte[] data = new byte[70000];
        for (int i = 0; i < data.length; i++) data[i] = (byte) i;
        AmqpWriter w = new AmqpWriter().longstr(data);
        assertEquals(4 + data.length, w.size());
        assertArrayEquals(data, new AmqpReader(w.toByteArray()).longstr());
        assertEquals("", new AmqpReader(new AmqpWriter().longstr("").toByteArray()).longstrAsString());
    }

    @Test
    void bitsPacking() throws Exception {
        assertArrayEquals(Hex.bytes("05"), new AmqpWriter().bits(true, false, true).toByteArray());
        assertArrayEquals(Hex.bytes("00"), new AmqpWriter().bits().toByteArray());
        assertArrayEquals(Hex.bytes("FF"), new AmqpWriter().bits(true, true, true, true, true, true, true, true).toByteArray());
        assertThrows(IllegalArgumentException.class, () -> new AmqpWriter().bits(new boolean[9]));
        boolean[] b = new AmqpReader(Hex.bytes("12")).bits(5);
        assertArrayEquals(new boolean[] {false, true, false, false, true}, b);
    }

    @Test
    void underflowRaisesProtocolException() {
        AmqpReader r = new AmqpReader(Hex.bytes("00 01"));
        assertThrows(ProtocolException.class, r::long_);
        AmqpReader r2 = new AmqpReader(Hex.bytes("05 61 62"));
        assertThrows(ProtocolException.class, r2::shortstr);
        AmqpReader r3 = new AmqpReader(Hex.bytes("00 00 00 10 61"));
        assertThrows(ProtocolException.class, r3::longstr);
    }

    @Test
    void writerGrows() {
        AmqpWriter w = new AmqpWriter(4);
        for (int i = 0; i < 1000; i++) w.octet(i);
        assertEquals(1000, w.size());
        assertTrue(w.toByteArray().length == 1000);
    }
}
