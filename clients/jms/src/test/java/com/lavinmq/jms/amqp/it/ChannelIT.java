package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.GetResponse;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.method.QueueMethods;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChannelIT {
    private AmqpConnection conn;

    @BeforeEach
    void connect() throws Exception {
        conn = Broker.connect();
    }

    @AfterEach
    void disconnect() throws Exception {
        conn.close();
    }

    @Test
    void declarePublishGetAck() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String q = Broker.uniqueName("chan");
        QueueMethods.DeclareOk ok = ch.queueDeclare(q, false, false, false, true, null);
        assertEquals(q, ok.queue());
        assertEquals(0, ok.messageCount());
        ch.basicPublish("", q, false, new BasicProperties.Builder().deliveryMode(2).messageId("m1").build(), "hello".getBytes(StandardCharsets.UTF_8));
        GetResponse g = null;
        for (int i = 0; i < 50 && g == null; i++) {
            g = ch.basicGet(q, false);
            if (g == null) Thread.sleep(20);
        }
        assertNotNull(g);
        assertEquals("hello", new String(g.body(), StandardCharsets.UTF_8));
        assertEquals("m1", g.properties().messageId());
        assertEquals(2, g.properties().deliveryMode());
        assertFalse(g.redelivered());
        ch.basicAck(g.deliveryTag(), false);
        assertNull(ch.basicGet(q, false));
        assertEquals(0, ch.queueDelete(q, false, false));
        ch.close();
        assertFalse(ch.isOpen());
    }

    @Test
    void serverNamedQueueAndPurge() throws Exception {
        AmqpChannel ch = conn.openChannel();
        QueueMethods.DeclareOk ok = ch.queueDeclare("", false, false, true, true, null);
        assertTrue(ok.queue().startsWith("amq.gen-"), ok.queue());
        ch.basicPublish("", ok.queue(), false, BasicProperties.EMPTY, new byte[0]);
        ch.basicPublish("", ok.queue(), false, BasicProperties.EMPTY, new byte[0]);
        Thread.sleep(100);
        assertEquals(2, ch.queuePurge(ok.queue()));
        ch.close();
    }

    @Test
    void passiveDeclareOfMissingQueueIs404AndKillsChannel() throws Exception {
        AmqpChannel ch = conn.openChannel();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ShutdownSignal> sig = new AtomicReference<>();
        ch.setListener(new ChannelListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                sig.set(signal);
                latch.countDown();
            }
        });
        ChannelClosedException e = assertThrows(ChannelClosedException.class,
            () -> ch.queueDeclare("no-such-queue-" + System.nanoTime(), true, false, false, false, null));
        assertEquals(404, e.replyCode());
        assertTrue(e.replyText().startsWith("NOT_FOUND"), e.replyText());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(404, sig.get().replyCode());
        assertFalse(sig.get().isHardError());
        assertFalse(ch.isOpen());
        assertThrows(ChannelClosedException.class, () -> ch.basicQos(1, false));
        // the connection is still fine and the id is reusable
        AmqpChannel ch2 = conn.openChannel();
        ch2.basicQos(10, false);
        ch2.close();
    }

    @Test
    void redeclareWithDifferentArgumentsIs406() throws Exception {
        String q = Broker.uniqueName("args");
        AmqpChannel ch = conn.openChannel();
        ch.queueDeclare(q, false, true, false, false, Map.of("x-max-priority", 5));
        AmqpChannel ch2 = conn.openChannel();
        ChannelClosedException e = assertThrows(ChannelClosedException.class,
            () -> ch2.queueDeclare(q, false, true, false, false, Map.of("x-max-priority", 7)));
        assertEquals(406, e.replyCode());
        ch.queueDelete(q, false, false);
        ch.close();
    }

    @Test
    void exchangeToExchangeBinding() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String e1 = Broker.uniqueName("e1");
        String e2 = Broker.uniqueName("e2");
        String q = Broker.uniqueName("q");
        ch.exchangeDeclare(e1, "topic", false, false, true, false, null);
        ch.exchangeDeclare(e2, "fanout", false, false, true, false, null);
        ch.exchangeBind(e2, e1, "a.#", null);
        ch.queueDeclare(q, false, false, true, true, null);
        ch.queueBind(q, e2, "", null);
        ch.basicPublish(e1, "a.b", false, BasicProperties.EMPTY, "x".getBytes());
        GetResponse g = null;
        for (int i = 0; i < 50 && g == null; i++) {
            g = ch.basicGet(q, true);
            if (g == null) Thread.sleep(20);
        }
        assertNotNull(g);
        assertEquals(e1, g.exchange());
        assertEquals("a.b", g.routingKey());
        ch.queueUnbind(q, e2, "", null);
        ch.exchangeUnbind(e2, e1, "a.#", null);
        ch.exchangeDelete(e1, false);
        ch.exchangeDelete(e2, false);
        ch.close();
    }

    @Test
    void manyChannels() throws Exception {
        AmqpChannel[] chs = new AmqpChannel[50];
        for (int i = 0; i < chs.length; i++) chs[i] = conn.openChannel();
        for (AmqpChannel ch : chs) ch.basicQos(1, false);
        for (AmqpChannel ch : chs) ch.close();
        AmqpChannel again = conn.openChannel();
        assertEquals(1, again.id());
        again.close();
    }
}
