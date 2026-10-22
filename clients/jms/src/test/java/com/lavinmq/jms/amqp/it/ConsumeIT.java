package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ConsumerCallback;
import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumeIT {
    private AmqpConnection conn;

    static final class Collector implements ConsumerCallback {
        final BlockingQueue<Delivery> deliveries = new LinkedBlockingQueue<>();
        final CountDownLatch cancelled = new CountDownLatch(1);
        final CountDownLatch shutdown = new CountDownLatch(1);
        final AtomicReference<ShutdownSignal> signal = new AtomicReference<>();

        @Override
        public void handleDelivery(Delivery d) {
            deliveries.add(d);
        }

        @Override
        public void handleCancel(String consumerTag) {
            cancelled.countDown();
        }

        @Override
        public void handleShutdown(String consumerTag, ShutdownSignal s) {
            signal.set(s);
            shutdown.countDown();
        }

        Delivery next() throws InterruptedException {
            Delivery d = deliveries.poll(5, TimeUnit.SECONDS);
            assertNotNull(d, "no delivery within 5s");
            return d;
        }
    }

    @BeforeEach
    void connect() throws Exception {
        conn = Broker.connect();
    }

    @AfterEach
    void disconnect() throws Exception {
        conn.close();
    }

    private static String text(Delivery d) {
        return new String(d.body(), StandardCharsets.UTF_8);
    }

    @Test
    void deliveriesInOrderAckNackReject() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        for (int i = 0; i < 5; i++) ch.basicPublish("", q, false, BasicProperties.EMPTY, ("m" + i).getBytes());
        Collector c = new Collector();
        String tag = ch.basicConsume(q, "", false, false, null, c);
        assertTrue(tag.startsWith("amq.ctag"), tag);
        Delivery d0 = c.next();
        assertEquals("m0", text(d0));
        assertEquals(tag, d0.consumerTag());
        ch.basicAck(d0.deliveryTag(), false);
        Delivery d1 = c.next();
        assertEquals("m1", text(d1));
        ch.basicNack(d1.deliveryTag(), false, true);
        Delivery d2 = c.next();
        assertEquals("m2", text(d2));
        ch.basicReject(d2.deliveryTag(), false);
        Delivery d3 = c.next();
        assertEquals("m3", text(d3));
        Delivery d4 = c.next();
        assertEquals("m4", text(d4));
        Delivery d1again = c.next();
        assertEquals("m1", text(d1again));
        assertTrue(d1again.redelivered());
        ch.basicAck(d4.deliveryTag(), true);
        ch.basicAck(d1again.deliveryTag(), false);
        ch.basicCancel(tag);
        ch.basicPublish("", q, false, BasicProperties.EMPTY, "after-cancel".getBytes());
        assertNull(c.deliveries.poll(300, TimeUnit.MILLISECONDS));
        ch.close();
    }

    @Test
    void prefetchIsHonoured() throws Exception {
        AmqpChannel ch = conn.openChannel();
        ch.basicQos(2, false);
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        for (int i = 0; i < 5; i++) ch.basicPublish("", q, false, BasicProperties.EMPTY, ("m" + i).getBytes());
        Collector c = new Collector();
        ch.basicConsume(q, "", false, false, null, c);
        Delivery a = c.next();
        Delivery b = c.next();
        assertNull(c.deliveries.poll(300, TimeUnit.MILLISECONDS));
        ch.basicAck(a.deliveryTag(), false);
        assertEquals("m2", text(c.next()));
        ch.basicAck(b.deliveryTag(), true);
        ch.close();
    }

    @Test
    void consumerTagChosenByClient() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        Collector c = new Collector();
        assertEquals("my-tag", ch.basicConsume(q, "my-tag", true, false, null, c));
        ch.basicPublish("", q, false, BasicProperties.EMPTY, "x".getBytes());
        assertEquals("my-tag", c.next().consumerTag());
        ch.close();
    }

    @Test
    void queueDeleteCancelsConsumer() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare(Broker.uniqueName("del"), false, false, false, false, null).queue();
        Collector c = new Collector();
        ch.basicConsume(q, "", true, false, null, c);
        ch.queueDelete(q, false, false);
        assertTrue(c.cancelled.await(5, TimeUnit.SECONDS));
        assertTrue(ch.isOpen());
        ch.close();
        assertTrue(ch.shutdownCause().isInitiatedByApplication());
    }

    @Test
    void channelCloseNotifiesConsumer() throws Exception {
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        Collector c = new Collector();
        ch.basicConsume(q, "", true, false, null, c);
        ch.close();
        assertTrue(c.shutdown.await(5, TimeUnit.SECONDS));
        assertTrue(c.signal.get().isInitiatedByApplication());
    }

    @Test
    void connectionCloseNotifiesConsumer() throws Exception {
        AmqpConnection other = Broker.connect();
        AmqpChannel ch = other.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        Collector c = new Collector();
        ch.basicConsume(q, "", true, false, null, c);
        other.close();
        assertTrue(c.shutdown.await(5, TimeUnit.SECONDS));
        assertTrue(c.signal.get().isHardError());
    }

    @Test
    void ackFromDeliveryCallback() throws Exception {
        AmqpChannel ch = conn.openChannel();
        ch.basicQos(1, false);
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        int n = 200;
        for (int i = 0; i < n; i++) ch.basicPublish("", q, false, BasicProperties.EMPTY, Integer.toString(i).getBytes());
        CountDownLatch done = new CountDownLatch(n);
        AtomicReference<Throwable> err = new AtomicReference<>();
        int[] expected = {0};
        ch.basicConsume(q, "", false, false, null, d -> {
            try {
                int v = Integer.parseInt(text(d));
                if (v != expected[0]) err.compareAndSet(null, new AssertionError("out of order: " + v + " expected " + expected[0]));
                expected[0]++;
                ch.basicAck(d.deliveryTag(), false);
            } catch (Exception e) {
                err.compareAndSet(null, e);
            } finally {
                done.countDown();
            }
        });
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertNull(err.get());
        ch.close();
    }
}
