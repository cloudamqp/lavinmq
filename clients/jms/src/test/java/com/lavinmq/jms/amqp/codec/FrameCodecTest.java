package com.lavinmq.jms.amqp.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.method.BasicMethods;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.util.List;
import org.junit.jupiter.api.Test;

class FrameCodecTest {
    private static byte[] write(Frame f) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        FrameCodec.write(new DataOutputStream(bos), f);
        return bos.toByteArray();
    }

    private static Frame read(byte[] b, int max) throws Exception {
        return FrameCodec.read(new DataInputStream(new ByteArrayInputStream(b)), max);
    }

    @Test
    void heartbeatGolden() throws Exception {
        assertEquals("08 00 00 00 00 00 00 CE", Hex.hex(write(Frame.heartbeat())));
        Frame f = read(Hex.bytes("08 00 00 00 00 00 00 CE"), 4096);
        assertEquals(FrameType.HEARTBEAT, f.type());
        assertEquals(0, f.channel());
        assertEquals(0, f.payload().length);
    }

    @Test
    void headerGolden() throws Exception {
        Frame h = Frame.header(1, 5, BasicProperties.EMPTY);
        assertEquals("00 3C 00 00 00 00 00 00 00 00 00 05 00 00", Hex.hex(h.payload()));
        assertEquals("02 00 01 00 00 00 0E 00 3C 00 00 00 00 00 00 00 00 00 05 00 00 CE", Hex.hex(write(h)));
        ContentHeader ch = ContentHeader.read(new AmqpReader(h.payload()));
        assertEquals(60, ch.classId());
        assertEquals(5, ch.bodySize());
    }

    @Test
    void errors() {
        assertThrows(ProtocolException.class, () -> read(Hex.bytes("08 00 00 00 00 00 00 CF"), 4096));
        assertThrows(ProtocolException.class, () -> read(Hex.bytes("07 00 00 00 00 00 00 CE"), 4096));
        assertThrows(EOFException.class, () -> read(Hex.bytes("01 00 01 00 00 00 10 00"), 4096));
        assertThrows(EOFException.class, () -> read(new byte[0], 4096));
        assertThrows(ProtocolException.class, () -> read(Hex.bytes("41 4D 51 50 00 00 09 01"), 4096));
    }

    @Test
    void frameMaxEnforcement() throws Exception {
        byte[] payload = new byte[4096 - FrameCodec.FRAME_OVERHEAD];
        Frame ok = new Frame(FrameType.BODY, 1, payload);
        assertEquals(ok, read(write(ok), 4096));
        Frame tooBig = new Frame(FrameType.BODY, 1, new byte[4096 - FrameCodec.FRAME_OVERHEAD + 1]);
        assertThrows(ProtocolException.class, () -> read(write(tooBig), 4096));
        assertEquals(tooBig, read(write(tooBig), 0));
    }

    @Test
    void splitBody() {
        assertEquals(1, FrameCodec.splitBody(1, new byte[4088], 4096).size());
        List<Frame> two = FrameCodec.splitBody(1, new byte[4089], 4096);
        assertEquals(2, two.size());
        assertEquals(4088, two.get(0).payload().length);
        assertEquals(1, two.get(1).payload().length);
        assertEquals(0, FrameCodec.splitBody(1, new byte[0], 4096).size());
        assertEquals(2, FrameCodec.splitBody(1, new byte[8176], 4096).size());
    }

    @Test
    void writeContentMatchesSeparateFrames() throws Exception {
        byte[] body = new byte[10000];
        for (int i = 0; i < body.length; i++) body[i] = (byte) i;
        BasicMethods.Publish pub = new BasicMethods.Publish("ex", "rk", true, false);
        BasicProperties props = new BasicProperties.Builder().deliveryMode(2).build();
        ByteArrayOutputStream a = new ByteArrayOutputStream();
        FrameCodec.writeContent(new DataOutputStream(a), 3, pub, props, body, 4096);
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(b);
        FrameCodec.write(out, Frame.method(3, pub));
        FrameCodec.write(out, Frame.header(3, body.length, props));
        for (Frame f : FrameCodec.splitBody(3, body, 4096)) FrameCodec.write(out, f);
        assertEquals(Hex.hex(b.toByteArray()), Hex.hex(a.toByteArray()));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(a.toByteArray()));
        assertEquals(FrameType.METHOD, FrameCodec.read(in, 4096).type());
        assertEquals(FrameType.HEADER, FrameCodec.read(in, 4096).type());
        int bodies = 0;
        int total = 0;
        while (in.available() > 0) {
            Frame f = FrameCodec.read(in, 4096);
            assertEquals(FrameType.BODY, f.type());
            total += f.payload().length;
            bodies++;
        }
        assertEquals(3, bodies);
        assertEquals(10000, total);
        assertTrue(Frame.method(3, pub).toString().contains("type=1"));
    }
}
