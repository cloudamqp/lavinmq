package com.lavinmq.jms.amqp.transport;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ConnectionListener;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameType;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class HeartbeatTest {
    private static AmqpConnection connect(FakeAmqpServer server, AtomicReference<FakeAmqpServer.Session> out) throws Exception {
        ConnectionConfig cfg = ConnectionConfig.builder().host("127.0.0.1").port(server.port()).heartbeatSeconds(1).build();
        CompletableFuture<AmqpConnection> f = CompletableFuture.supplyAsync(() -> {
            try {
                return AmqpConnection.open(cfg);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        FakeAmqpServer.Session s = server.accept();
        s.handshake(2048, 131072, 1);
        out.set(s);
        return f.get(5, TimeUnit.SECONDS);
    }

    @Test
    void clientSendsHeartbeatsWhenIdle() throws Exception {
        try (FakeAmqpServer server = new FakeAmqpServer()) {
            AtomicReference<FakeAmqpServer.Session> s = new AtomicReference<>();
            AmqpConnection c = connect(server, s);
            int heartbeats = 0;
            long deadline = System.currentTimeMillis() + 1800;
            while (System.currentTimeMillis() < deadline) {
                FakeAmqpServer.Received r = s.get().poll(300);
                if (r != null && r.frame().type() == FrameType.HEARTBEAT) heartbeats++;
                s.get().send(Frame.heartbeat());
            }
            assertTrue(heartbeats >= 2, "heartbeats: " + heartbeats);
            assertTrue(c.isOpen());
            c.abort();
        }
    }

    @Test
    void silentServerIsDetected() throws Exception {
        try (FakeAmqpServer server = new FakeAmqpServer()) {
            AtomicReference<FakeAmqpServer.Session> s = new AtomicReference<>();
            AmqpConnection c = connect(server, s);
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<ShutdownSignal> sig = new AtomicReference<>();
            c.addListener(new ConnectionListener() {
                @Override
                public void onShutdown(ShutdownSignal signal) {
                    sig.set(signal);
                    latch.countDown();
                }
            });
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertFalse(c.isOpen());
            assertNotNull(sig.get().cause());
            assertTrue(sig.get().cause() instanceof SocketTimeoutException, sig.get().cause().toString());
        }
    }
}
