package com.lavinmq.jms.amqp.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ConnectionListener;
import com.lavinmq.jms.amqp.ConsumerCallback;
import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.ReturnedMessage;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.method.BasicMethods;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DispatchTest {
    private FakeAmqpServer server;
    private FakeAmqpServer.Session session;
    private AmqpConnection conn;

    @BeforeEach
    void start() throws Exception {
        server = new FakeAmqpServer();
        ConnectionConfig cfg = ConnectionConfig.builder().host("127.0.0.1").port(server.port()).heartbeatSeconds(0).rpcTimeoutMs(5000).build();
        CompletableFuture<AmqpConnection> f = CompletableFuture.supplyAsync(() -> {
            try {
                return AmqpConnection.open(cfg);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session = server.accept();
        session.handshake();
        conn = f.get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void stop() throws Exception {
        conn.abort();
        server.close();
    }

    private AmqpChannel openChannel() throws Exception {
        CompletableFuture<AmqpChannel> f = CompletableFuture.supplyAsync(() -> {
            try {
                return conn.openChannel();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.acceptChannel();
        return f.get(5, TimeUnit.SECONDS);
    }

    private String consume(AmqpChannel ch, String tag, ConsumerCallback cb) throws Exception {
        CompletableFuture<String> f = CompletableFuture.supplyAsync(() -> {
            try {
                return ch.basicConsume("q", tag, false, false, null, cb);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.expect(BasicMethods.Consume.class);
        session.send(ch.id(), new BasicMethods.ConsumeOk(tag));
        return f.get(5, TimeUnit.SECONDS);
    }

    private void deliver(int channel, String tag, long deliveryTag) throws IOException {
        session.sendContent(channel, new BasicMethods.Deliver(tag, deliveryTag, false, "", "q"), BasicProperties.EMPTY,
            Long.toString(deliveryTag).getBytes());
    }

    @Test
    void deliveriesAreOrderedPerChannelAndChannelsAreIndependent() throws Exception {
        AmqpChannel ch1 = openChannel();
        AmqpChannel ch2 = openChannel();
        List<Long> got1 = Collections.synchronizedList(new ArrayList<>());
        List<Long> got2 = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch block = new CountDownLatch(1);
        CountDownLatch firstOnCh1 = new CountDownLatch(1);
        CountDownLatch done2 = new CountDownLatch(1000);
        java.util.concurrent.atomic.AtomicInteger started1 = new java.util.concurrent.atomic.AtomicInteger();
        consume(ch1, "c1", d -> {
            if (started1.incrementAndGet() == 1) {
                firstOnCh1.countDown();
                try {
                    block.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            got1.add(d.deliveryTag());
        });
        consume(ch2, "c2", d -> {
            got2.add(d.deliveryTag());
            done2.countDown();
        });
        for (int i = 1; i <= 1000; i++) {
            deliver(ch1.id(), "c1", i);
            deliver(ch2.id(), "c2", i);
        }
        assertTrue(firstOnCh1.await(5, TimeUnit.SECONDS));
        // channel 1's consumer is blocked; channel 2 still gets everything and the reader still serves RPCs
        assertTrue(done2.await(5, TimeUnit.SECONDS));
        assertEquals(1, started1.get());
        assertEquals(0, got1.size());
        CompletableFuture<Void> qos = CompletableFuture.runAsync(() -> {
            try {
                ch2.basicQos(5, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.expect(BasicMethods.Qos.class);
        session.send(ch2.id(), new BasicMethods.QosOk());
        qos.get(5, TimeUnit.SECONDS);
        block.countDown();
        long deadline = System.currentTimeMillis() + 5000;
        while (got1.size() < 1000 && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(1000, got1.size());
        for (int i = 0; i < 1000; i++) {
            assertEquals(i + 1, got1.get(i));
            assertEquals(i + 1, got2.get(i));
        }
    }

    @Test
    void serverCancelReturnConfirmFlowBlockedAndShutdownOrdering() throws Exception {
        AmqpChannel ch = openChannel();
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        ch.setListener(new ChannelListener() {
            @Override
            public void onReturn(ReturnedMessage r) {
                events.add("return:" + r.replyCode() + ":" + new String(r.body()));
            }

            @Override
            public void onConfirm(boolean ack, long deliveryTag, boolean multiple) {
                events.add("confirm:" + ack + ":" + deliveryTag + ":" + multiple);
            }

            @Override
            public void onFlow(boolean active) {
                events.add("flow:" + active);
            }

            @Override
            public void onShutdown(ShutdownSignal signal) {
                events.add("shutdown:" + signal.replyCode());
            }
        });
        consume(ch, "c1", new ConsumerCallback() {
            @Override
            public void handleDelivery(Delivery d) {
                events.add("deliver:" + d.deliveryTag());
            }

            @Override
            public void handleCancel(String consumerTag) {
                events.add("cancel:" + consumerTag);
            }

            @Override
            public void handleShutdown(String consumerTag, ShutdownSignal signal) {
                events.add("consumer-shutdown:" + consumerTag);
            }
        });
        conn.addListener(new ConnectionListener() {
            @Override
            public void onBlocked(String reason) {
                events.add("blocked:" + reason);
            }

            @Override
            public void onUnblocked() {
                events.add("unblocked");
            }
        });
        deliver(ch.id(), "c1", 1);
        session.sendContent(ch.id(), new BasicMethods.Return(312, "NO_ROUTE", "", "nowhere"), BasicProperties.EMPTY, "lost".getBytes());
        session.send(ch.id(), new BasicMethods.Ack(7, true));
        session.send(ch.id(), new BasicMethods.Nack(8, false, false));
        session.send(ch.id(), new com.lavinmq.jms.amqp.method.ChannelMethods.Flow(false));
        session.expect(com.lavinmq.jms.amqp.method.ChannelMethods.FlowOk.class);
        session.send(ch.id(), new BasicMethods.Cancel("c1", true));
        assertEquals("deliver:1", events.poll(5, TimeUnit.SECONDS));
        assertEquals("return:312:lost", events.poll(5, TimeUnit.SECONDS));
        assertEquals("confirm:true:7:true", events.poll(5, TimeUnit.SECONDS));
        assertEquals("confirm:false:8:false", events.poll(5, TimeUnit.SECONDS));
        assertEquals("flow:false", events.poll(5, TimeUnit.SECONDS));
        assertEquals("cancel:c1", events.poll(5, TimeUnit.SECONDS));
        // connection-level events use their own dispatcher, so they are only ordered among themselves
        session.send(0, new ConnectionMethods.Blocked("low on disk"));
        session.send(0, new ConnectionMethods.Unblocked());
        assertEquals("blocked:low on disk", events.poll(5, TimeUnit.SECONDS));
        assertEquals("unblocked", events.poll(5, TimeUnit.SECONDS));
        // no cancel-ok must be sent for no_wait cancels
        assertNull(session.poll(200));

        // deliveries queued before a close are dispatched before the shutdown callback
        consume(ch, "c2", d -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            events.add("deliver:" + d.deliveryTag());
        });
        deliver(ch.id(), "c2", 2);
        deliver(ch.id(), "c2", 3);
        session.send(ch.id(), new com.lavinmq.jms.amqp.method.ChannelMethods.Close(406, "PRECONDITION_FAILED", 0, 0));
        assertEquals("deliver:2", events.poll(5, TimeUnit.SECONDS));
        assertEquals("deliver:3", events.poll(5, TimeUnit.SECONDS));
        assertEquals("shutdown:406", events.poll(5, TimeUnit.SECONDS));
        assertNotNull(session.expect(com.lavinmq.jms.amqp.method.ChannelMethods.CloseOk.class));
    }

    @Test
    void basicGetCollectsBody() throws Exception {
        AmqpChannel ch = openChannel();
        CompletableFuture<com.lavinmq.jms.amqp.GetResponse> f = CompletableFuture.supplyAsync(() -> {
            try {
                return ch.basicGet("q", false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.expect(BasicMethods.Get.class);
        byte[] body = new byte[FakeAmqpServer.FRAME_MAX * 2 + 5];
        session.sendContent(ch.id(), new BasicMethods.GetOk(42, true, "", "q", 3), new BasicProperties.Builder().type("t").build(), body);
        com.lavinmq.jms.amqp.GetResponse g = f.get(5, TimeUnit.SECONDS);
        assertEquals(42, g.deliveryTag());
        assertEquals(3, g.messageCount());
        assertEquals("t", g.properties().type());
        assertEquals(body.length, g.body().length);
        CompletableFuture<com.lavinmq.jms.amqp.GetResponse> f2 = CompletableFuture.supplyAsync(() -> {
            try {
                return ch.basicGet("q", false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.expect(BasicMethods.Get.class);
        session.send(ch.id(), new BasicMethods.GetEmpty());
        assertNull(f2.get(5, TimeUnit.SECONDS));
        AtomicReference<Object> unused = new AtomicReference<>();
        assertNull(unused.get());
    }
}
