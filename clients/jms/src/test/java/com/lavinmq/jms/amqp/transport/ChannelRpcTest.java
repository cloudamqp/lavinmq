package com.lavinmq.jms.amqp.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ConnectionListener;
import com.lavinmq.jms.amqp.RpcTimeoutException;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.method.ChannelMethods;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import com.lavinmq.jms.amqp.method.QueueMethods;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChannelRpcTest {
    private FakeAmqpServer server;
    private FakeAmqpServer.Session session;
    private AmqpConnection conn;

    @BeforeEach
    void start() throws Exception {
        server = new FakeAmqpServer();
        ConnectionConfig cfg = ConnectionConfig.builder().host("127.0.0.1").port(server.port()).heartbeatSeconds(0).rpcTimeoutMs(500).build();
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

    @Test
    void serverChannelCloseFailsPendingRpcPromptly() throws Exception {
        AmqpChannel ch = openChannel();
        CountDownLatch shutdown = new CountDownLatch(1);
        AtomicReference<ShutdownSignal> sig = new AtomicReference<>();
        ch.setListener(new ChannelListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                sig.set(signal);
                shutdown.countDown();
            }
        });
        CompletableFuture<Throwable> result = CompletableFuture.supplyAsync(() -> {
            try {
                ch.queueDeclare("missing", true, false, false, false, null);
                return null;
            } catch (IOException e) {
                return e;
            }
        });
        FakeAmqpServer.Received r = session.expect(QueueMethods.Declare.class);
        assertEquals(ch.id(), r.channel());
        long t0 = System.nanoTime();
        session.send(ch.id(), new ChannelMethods.Close(404, "NOT_FOUND - no queue 'missing' in vhost '/'", 50, 10));
        Throwable t = result.get(5, TimeUnit.SECONDS);
        assertTrue(System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(400), "rpc did not fail promptly");
        ChannelClosedException e = (ChannelClosedException) t;
        assertEquals(404, e.replyCode());
        assertEquals("NOT_FOUND - no queue 'missing' in vhost '/'", e.replyText());
        FakeAmqpServer.Received closeOk = session.expectAny();
        assertEquals(ch.id(), closeOk.channel());
        closeOk.as(ChannelMethods.CloseOk.class);
        assertTrue(shutdown.await(5, TimeUnit.SECONDS));
        assertEquals(404, sig.get().replyCode());
        assertFalse(ch.isOpen());
        assertTrue(conn.isOpen());
        assertThrows(ChannelClosedException.class, () -> ch.basicQos(1, false));
    }

    @Test
    void connectionCloseFailsPendingRpcAndNotifiesEveryChannel() throws Exception {
        AmqpChannel ch1 = openChannel();
        AmqpChannel ch2 = openChannel();
        CountDownLatch shutdowns = new CountDownLatch(3);
        ChannelListener l = new ChannelListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                assertTrue(signal.isHardError());
                shutdowns.countDown();
            }
        };
        ch1.setListener(l);
        ch2.setListener(l);
        conn.addListener(new ConnectionListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                shutdowns.countDown();
            }
        });
        CompletableFuture<Throwable> result = CompletableFuture.supplyAsync(() -> {
            try {
                ch1.queueDeclare("q", false, false, false, false, null);
                return null;
            } catch (IOException e) {
                return e;
            }
        });
        session.expect(QueueMethods.Declare.class);
        session.send(0, new ConnectionMethods.Close(320, "CONNECTION_FORCED - shutting down", 0, 0));
        Throwable t = result.get(5, TimeUnit.SECONDS);
        ConnectionClosedException e = (ConnectionClosedException) t;
        assertEquals(320, e.replyCode());
        session.expect(ConnectionMethods.CloseOk.class);
        assertTrue(shutdowns.await(5, TimeUnit.SECONDS));
        assertFalse(conn.isOpen());
        assertFalse(ch2.isOpen());
        assertThrows(ConnectionClosedException.class, () -> ch2.basicQos(1, false));
        assertThrows(ConnectionClosedException.class, conn::openChannel);
    }

    @Test
    void rpcTimeoutAbortsTheChannel() throws Exception {
        AmqpChannel ch = openChannel();
        long t0 = System.currentTimeMillis();
        assertThrows(RpcTimeoutException.class, () -> ch.basicQos(1, false));
        assertTrue(System.currentTimeMillis() - t0 >= 400);
        assertFalse(ch.isOpen());
        assertTrue(conn.isOpen());
        session.expect(com.lavinmq.jms.amqp.method.BasicMethods.Qos.class);
        session.expect(ChannelMethods.Close.class);
        assertThrows(ChannelClosedException.class, () -> ch.basicQos(1, false));
    }

    @Test
    void gracefulChannelAndConnectionClose() throws Exception {
        AmqpChannel ch = openChannel();
        CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
            try {
                ch.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        FakeAmqpServer.Received r = session.expect(ChannelMethods.Close.class);
        assertEquals(200, r.as(ChannelMethods.Close.class).replyCode());
        session.send(ch.id(), new ChannelMethods.CloseOk());
        closing.get(5, TimeUnit.SECONDS);
        assertFalse(ch.isOpen());
        assertTrue(ch.shutdownCause().isInitiatedByApplication());
        // the id can be reused
        AmqpChannel again = openChannel();
        assertEquals(ch.id(), again.id());

        CompletableFuture<Void> closingConn = CompletableFuture.runAsync(() -> {
            try {
                conn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        session.acceptClose();
        closingConn.get(5, TimeUnit.SECONDS);
        assertFalse(conn.isOpen());
        assertFalse(again.isOpen());
        assertTrue(session.awaitClientClose(2000));
    }

    @Test
    void unexpectedMethodIsProtocolError() throws Exception {
        AmqpChannel ch = openChannel();
        CountDownLatch shutdown = new CountDownLatch(1);
        AtomicReference<ShutdownSignal> sig = new AtomicReference<>();
        conn.addListener(new ConnectionListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                sig.set(signal);
                shutdown.countDown();
            }
        });
        session.send(ch.id(), new QueueMethods.DeclareOk("q", 0, 0));
        FakeAmqpServer.Received r = session.expect(ConnectionMethods.Close.class);
        assertEquals(501, r.as(ConnectionMethods.Close.class).replyCode());
        assertTrue(shutdown.await(5, TimeUnit.SECONDS));
        assertTrue(sig.get().cause() instanceof com.lavinmq.jms.amqp.ProtocolException);
    }

    @Test
    void noFreeChannelIds() throws Exception {
        // channelMax negotiated as 2047 by the client against a server offering 2048; just check the allocator path
        ChannelTable t = new ChannelTable(2);
        assertEquals(1, t.allocate());
        assertEquals(2, t.allocate());
        assertEquals(-1, t.allocate());
        t.free(1);
        assertEquals(1, t.allocate());
        ExecutionException unused = null;
        assertEquals(null, unused);
    }
}
