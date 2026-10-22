package com.lavinmq.jms.amqp.method;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.Hex;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class MethodCodecTest {
    private static byte[] encode(Method m) {
        return Frame.method(1, m).payload();
    }

    @Test
    void tuneOkGolden() throws Exception {
        assertEquals("00 0A 00 1F 07 FF 00 02 00 00 00 3C", Hex.hex(encode(new ConnectionMethods.TuneOk(2047, 131072, 60))));
        assertEquals(new ConnectionMethods.Tune(2048, 131072, 300),
            MethodDecoder.decode(Hex.bytes("00 0A 00 1E 08 00 00 02 00 00 01 2C")));
    }

    @Test
    void openGolden() throws Exception {
        assertEquals("00 0A 00 28 01 2F 00 00", Hex.hex(encode(new ConnectionMethods.Open("/"))));
        assertEquals("00 14 00 0A 00", Hex.hex(encode(new ChannelMethods.Open())));
    }

    @Test
    void startOkGolden() throws Exception {
        ConnectionMethods.StartOk so = new ConnectionMethods.StartOk(Map.of("product", "x"), "PLAIN", "\0guest\0guest".getBytes(), "en_US");
        byte[] b = encode(so);
        assertEquals("00 0A 00 0B", Hex.hex(new byte[] {b[0], b[1], b[2], b[3]}));
        ConnectionMethods.StartOk back = (ConnectionMethods.StartOk) MethodDecoder.decode(b);
        assertEquals("PLAIN", back.mechanism());
        assertArrayEquals("\0guest\0guest".getBytes(), back.response());
        assertEquals("en_US", back.locale());
        assertEquals("x", back.clientProperties().get("product"));
    }

    @Test
    void bitPacking() {
        byte[] decl = encode(new ExchangeMethods.Declare("e", "topic", false, true, false, false, true, null));
        // 2 + 2 + 2 (reserved) + 2 ("e") + 6 ("topic") = offset 14 for bits
        assertEquals(0x12, decl[14] & 0xFF);
        byte[] pub = encode(new BasicMethods.Publish("", "q", true, false));
        assertEquals(0x01, pub[pub.length - 1] & 0xFF);
        byte[] nack = encode(new BasicMethods.Nack(7, false, true));
        assertEquals(0x02, nack[nack.length - 1] & 0xFF);
        byte[] consume = encode(new BasicMethods.Consume("q", "", false, true, true, false, null));
        assertEquals(0x06, consume[consume.length - 5] & 0xFF);
    }

    @Test
    void unbindOkIs51() throws Exception {
        assertEquals("00 28 00 33", Hex.hex(encode(new ExchangeMethods.UnbindOk())));
        assertTrue(MethodDecoder.decode(Hex.bytes("00 28 00 33")) instanceof ExchangeMethods.UnbindOk);
        assertThrows(ProtocolException.class, () -> MethodDecoder.decode(Hex.bytes("00 28 00 29")));
    }

    @Test
    void decodeServerMethods() throws Exception {
        Method close = MethodDecoder.decode(encode(new ChannelMethods.Close(404, "NOT_FOUND - no queue 'x'", 50, 10)));
        ChannelMethods.Close c = (ChannelMethods.Close) close;
        assertEquals(404, c.replyCode());
        assertEquals("NOT_FOUND - no queue 'x'", c.replyText());
        assertEquals(50, c.failingClassId());
        assertEquals(10, c.failingMethodId());
        BasicMethods.Deliver d = (BasicMethods.Deliver) MethodDecoder.decode(encode(new BasicMethods.Deliver("ctag", 9, true, "ex", "rk")));
        assertEquals("ctag", d.consumerTag());
        assertEquals(9, d.deliveryTag());
        assertTrue(d.redelivered());
        assertTrue(d.hasContent());
    }

    @Test
    void unknownIds() {
        assertThrows(ProtocolException.class, () -> MethodDecoder.decode(Hex.bytes("00 63 00 0A")));
        assertThrows(ProtocolException.class, () -> MethodDecoder.decode(Hex.bytes("00 3C 00 63")));
        assertThrows(ProtocolException.class, () -> MethodDecoder.decode(Hex.bytes("00 3C")));
    }

    static Stream<Method> allMethods() {
        Map<String, Object> args = Map.of("k", 1);
        return Stream.of(
            new ConnectionMethods.Start(0, 9, Map.of("product", "LavinMQ"), "PLAIN AMQPLAIN", "en_US"),
            new ConnectionMethods.StartOk(Map.of(), "PLAIN", new byte[] {0, 1}, "en_US"),
            new ConnectionMethods.Tune(1, 2, 3), new ConnectionMethods.TuneOk(4, 5, 6),
            new ConnectionMethods.Open("/vh"), new ConnectionMethods.OpenOk(),
            new ConnectionMethods.Close(320, "CONNECTION_FORCED", 0, 0), new ConnectionMethods.CloseOk(),
            new ConnectionMethods.Blocked("low on disk"), new ConnectionMethods.Unblocked(),
            new ConnectionMethods.UpdateSecret(new byte[] {7}, "r"), new ConnectionMethods.UpdateSecretOk(),
            new ChannelMethods.Open(), new ChannelMethods.OpenOk(), new ChannelMethods.Flow(true), new ChannelMethods.FlowOk(false),
            new ChannelMethods.Close(406, "PRECONDITION_FAILED", 50, 10), new ChannelMethods.CloseOk(),
            new ExchangeMethods.Declare("e", "direct", true, true, true, true, true, args), new ExchangeMethods.DeclareOk(),
            new ExchangeMethods.Delete("e", true, false), new ExchangeMethods.DeleteOk(),
            new ExchangeMethods.Bind("d", "s", "rk", false, args), new ExchangeMethods.BindOk(),
            new ExchangeMethods.Unbind("d", "s", "rk", true, args), new ExchangeMethods.UnbindOk(),
            new QueueMethods.Declare("q", false, true, false, true, false, args), new QueueMethods.DeclareOk("q", 10, 2),
            new QueueMethods.Bind("q", "e", "rk", false, args), new QueueMethods.BindOk(),
            new QueueMethods.Purge("q", true), new QueueMethods.PurgeOk(3),
            new QueueMethods.Delete("q", true, false, true), new QueueMethods.DeleteOk(4),
            new QueueMethods.Unbind("q", "e", "rk", args), new QueueMethods.UnbindOk(),
            new BasicMethods.Qos(0, 100, false), new BasicMethods.QosOk(),
            new BasicMethods.Consume("q", "t", true, false, true, false, args), new BasicMethods.ConsumeOk("t"),
            new BasicMethods.Cancel("t", true), new BasicMethods.CancelOk("t"),
            new BasicMethods.Publish("e", "rk", false, true), new BasicMethods.Return(312, "NO_ROUTE", "e", "rk"),
            new BasicMethods.Deliver("t", Long.MAX_VALUE, false, "e", "rk"), new BasicMethods.Get("q", true),
            new BasicMethods.GetOk(5, true, "e", "rk", 7), new BasicMethods.GetEmpty(),
            new BasicMethods.Ack(1, true), new BasicMethods.Reject(2, false), new BasicMethods.Recover(true),
            new BasicMethods.RecoverOk(), new BasicMethods.Nack(3, true, true),
            new ConfirmMethods.Select(false), new ConfirmMethods.SelectOk(),
            new TxMethods.Select(), new TxMethods.SelectOk(), new TxMethods.Commit(), new TxMethods.CommitOk(),
            new TxMethods.Rollback(), new TxMethods.RollbackOk());
    }

    @ParameterizedTest
    @MethodSource("allMethods")
    void everyMethodRoundTrips(Method m) throws Exception {
        Method back = MethodDecoder.decode(encode(m));
        assertEquals(m.getClass(), back.getClass());
        assertEquals(m.classId(), back.classId());
        assertEquals(m.methodId(), back.methodId());
        if (m instanceof ConnectionMethods.StartOk) {
            assertArrayEquals(((ConnectionMethods.StartOk) m).response(), ((ConnectionMethods.StartOk) back).response());
        } else if (m instanceof ConnectionMethods.UpdateSecret) {
            assertArrayEquals(((ConnectionMethods.UpdateSecret) m).secret(), ((ConnectionMethods.UpdateSecret) back).secret());
        } else {
            assertEquals(m, back);
        }
        assertEquals(List.of(), List.of()); // keep static analysis quiet about unused imports
    }
}
