package com.lavinmq.jms.amqp.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lavinmq.jms.amqp.ProtocolException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BasicPropertiesTest {
    private static byte[] encode(BasicProperties p) {
        AmqpWriter w = new AmqpWriter();
        p.write(w);
        return w.toByteArray();
    }

    @Test
    void emptyIsTwoZeroBytes() throws Exception {
        assertEquals("00 00", Hex.hex(encode(BasicProperties.EMPTY)));
        assertEquals(BasicProperties.EMPTY, BasicProperties.read(new AmqpReader(Hex.bytes("00 00"))));
        assertNull(BasicProperties.EMPTY.headers());
    }

    @Test
    void subsetGolden() throws Exception {
        BasicProperties p = new BasicProperties.Builder().deliveryMode(2).priority(0).build();
        assertEquals("18 00 02 00", Hex.hex(encode(p)));
        assertEquals(p, BasicProperties.read(new AmqpReader(Hex.bytes("18 00 02 00"))));
    }

    @Test
    void allFieldsRoundTrip() throws Exception {
        BasicProperties p = new BasicProperties.Builder()
            .contentType("text/plain").contentEncoding("utf-8").headers(Map.of("k", "v")).deliveryMode(2).priority(5)
            .correlationId("c1").replyTo("r1").expiration("1000").messageId("m1").timestamp(1700000000L)
            .type("t").userId("u").appId("a").clusterId("cl").build();
        assertEquals(0xFFFC, p.flags());
        byte[] b = encode(p);
        assertEquals("FF FC", Hex.hex(new byte[] {b[0], b[1]}));
        BasicProperties back = BasicProperties.read(new AmqpReader(b));
        assertEquals(p, back);
        assertEquals(p.hashCode(), back.hashCode());
        assertEquals("v", back.headers().get("k"));
    }

    @Test
    void invalidFlagBits() {
        assertThrows(ProtocolException.class, () -> BasicProperties.read(new AmqpReader(Hex.bytes("00 01"))));
        assertThrows(ProtocolException.class, () -> BasicProperties.read(new AmqpReader(Hex.bytes("00 02"))));
    }

    @Test
    void builderCopies() {
        BasicProperties p = new BasicProperties.Builder().messageId("x").build();
        BasicProperties q = p.builder().priority(3).build();
        assertEquals("x", q.messageId());
        assertEquals(3, q.priority());
        assertNull(p.priority());
    }
}
