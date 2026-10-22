package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.GetResponse;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LargeMessageIT {
    @Test
    void multiFrameBodyAndAllPropertiesRoundTrip() throws Exception {
        AmqpConnection conn = Broker.connect();
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        byte[] body = new byte[3 * conn.frameMax() + 1];
        for (int i = 0; i < body.length; i++) body[i] = (byte) (i * 31);
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("bool", true);
        headers.put("byte", (byte) 1);
        headers.put("short", (short) 2);
        headers.put("int", 3);
        headers.put("long", 4L);
        headers.put("float", 5.5f);
        headers.put("double", 6.25d);
        headers.put("string", "seven");
        headers.put("bytes", new byte[] {8});
        headers.put("null", null);
        headers.put("ts", Instant.ofEpochSecond(9));
        headers.put("dec", new BigDecimal("10.10"));
        headers.put("list", Arrays.asList(11, "x"));
        headers.put("map", Map.of("k", "v"));
        BasicProperties props = new BasicProperties.Builder()
            .contentType("application/octet-stream").contentEncoding("identity").headers(headers).deliveryMode(2).priority(4)
            .correlationId("corr").replyTo("reply").expiration("60000").messageId("mid").timestamp(1700000000L)
            .type("typ").userId("guest").appId("app").build();
        ch.basicPublish("", q, false, props, body);
        GetResponse g = null;
        for (int i = 0; i < 100 && g == null; i++) {
            g = ch.basicGet(q, false);
            if (g == null) Thread.sleep(20);
        }
        assertNotNull(g);
        assertArrayEquals(body, g.body());
        BasicProperties p = g.properties();
        assertEquals("application/octet-stream", p.contentType());
        assertEquals("identity", p.contentEncoding());
        assertEquals(2, p.deliveryMode());
        assertEquals(4, p.priority());
        assertEquals("corr", p.correlationId());
        assertEquals("reply", p.replyTo());
        assertEquals("60000", p.expiration());
        assertEquals("mid", p.messageId());
        assertEquals(1700000000L, p.timestamp());
        assertEquals("typ", p.type());
        assertEquals("guest", p.userId());
        assertEquals("app", p.appId());
        Map<String, Object> h = p.headers();
        assertEquals(true, h.get("bool"));
        assertEquals((byte) 1, h.get("byte"));
        assertEquals((short) 2, h.get("short"));
        assertEquals(3, h.get("int"));
        assertEquals(4L, h.get("long"));
        assertEquals(5.5f, h.get("float"));
        assertEquals(6.25d, h.get("double"));
        assertEquals("seven", h.get("string"));
        assertArrayEquals(new byte[] {8}, (byte[]) h.get("bytes"));
        assertEquals(Instant.ofEpochSecond(9), h.get("ts"));
        assertEquals(new BigDecimal("10.10"), h.get("dec"));
        assertEquals(List.of(11, "x"), h.get("list"));
        assertEquals(Map.of("k", "v"), h.get("map"));
        ch.basicNack(g.deliveryTag(), false, true);

        // and via a consumer
        BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
        ch.basicConsume(q, "", true, false, null, deliveries::add);
        Delivery d = deliveries.poll(5, TimeUnit.SECONDS);
        assertNotNull(d);
        assertArrayEquals(body, d.body());
        assertArrayEquals(new byte[] {8}, (byte[]) d.properties().headers().get("bytes"));
        assertEquals(p.builder().headers(null).build(), d.properties().builder().headers(null).build());
        assertEquals(p.headers().keySet(), d.properties().headers().keySet());
        ch.close();
        conn.close();
    }
}
