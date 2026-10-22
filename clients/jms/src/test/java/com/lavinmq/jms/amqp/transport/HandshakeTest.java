package com.lavinmq.jms.amqp.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.AmqpException;
import com.lavinmq.jms.amqp.AuthenticationFailureException;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.FrameCodec;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HandshakeTest {
    private FakeAmqpServer server;

    @BeforeEach
    void start() throws IOException {
        server = new FakeAmqpServer();
    }

    @AfterEach
    void stop() throws IOException {
        server.close();
    }

    private ConnectionConfig.Builder config() {
        return ConnectionConfig.builder().host("127.0.0.1").port(server.port()).username("u").password("p").vhost("vh")
            .connectionTimeoutMs(5000).rpcTimeoutMs(5000).heartbeatSeconds(0);
    }

    private CompletableFuture<AmqpConnection> openAsync(ConnectionConfig cfg) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return AmqpConnection.open(cfg);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Throwable unwrap(ExecutionException e) {
        Throwable c = e.getCause();
        return c instanceof RuntimeException && c.getCause() != null ? c.getCause() : c;
    }

    @Test
    void happyPathSendsExpectedFrames() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().connectionName("test-conn").requestedChannelMax(100).requestedFrameMax(8192).heartbeatSeconds(30).build());
        try (FakeAmqpServer.Session s = server.accept()) {
            assertTrue(s.sawProtocolHeader());
            s.send(0, new ConnectionMethods.Start(0, 9, Map.of("product", "FakeMQ"), "PLAIN AMQPLAIN", "en_US"));
            ConnectionMethods.StartOk startOk = s.expect(ConnectionMethods.StartOk.class).as(ConnectionMethods.StartOk.class);
            assertEquals("PLAIN", startOk.mechanism());
            assertEquals("\0u\0p", new String(startOk.response()));
            assertEquals("en_US", startOk.locale());
            assertEquals(ConnectionConfig.PRODUCT, startOk.clientProperties().get("product"));
            assertEquals("test-conn", startOk.clientProperties().get("connection_name"));
            @SuppressWarnings("unchecked")
            Map<String, Object> caps = (Map<String, Object>) startOk.clientProperties().get("capabilities");
            assertEquals(true, caps.get("authentication_failure_close"));
            assertEquals(true, caps.get("publisher_confirms"));
            assertEquals(true, caps.get("consumer_cancel_notify"));
            s.send(0, new ConnectionMethods.Tune(2048, 131072, 60));
            ConnectionMethods.TuneOk tuneOk = s.expect(ConnectionMethods.TuneOk.class).as(ConnectionMethods.TuneOk.class);
            assertEquals(100, tuneOk.channelMax());
            assertEquals(8192, tuneOk.frameMax());
            assertEquals(30, tuneOk.heartbeat());
            ConnectionMethods.Open open = s.expect(ConnectionMethods.Open.class).as(ConnectionMethods.Open.class);
            assertEquals("vh", open.virtualHost());
            s.send(0, new ConnectionMethods.OpenOk());
            AmqpConnection c = f.get(5, TimeUnit.SECONDS);
            assertEquals(100, c.channelMax());
            assertEquals(8192, c.frameMax());
            assertEquals(30, c.heartbeatSeconds());
            assertEquals("FakeMQ", c.serverProperties().get("product"));
            c.abort();
        }
    }

    @Test
    void negotiationRules() {
        assertEquals(2047, Handshake.negotiateMin(2047, 0));
        assertEquals(2048, Handshake.negotiateMin(0, 2048));
        assertEquals(100, Handshake.negotiateMin(100, 2048));
        assertEquals(60, Handshake.negotiateHeartbeat(60, 0));
        assertEquals(300, Handshake.negotiateHeartbeat(0, 300));
        assertEquals(0, Handshake.negotiateHeartbeat(0, 0));
        assertEquals(60, Handshake.negotiateHeartbeat(60, 300));
    }

    @Test
    void close403AfterStartOkIsAuthenticationFailure() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.send(0, new ConnectionMethods.Start(0, 9, Map.of(), "PLAIN", "en_US"));
            s.expect(ConnectionMethods.StartOk.class);
            s.send(0, new ConnectionMethods.Close(403, "ACCESS_REFUSED - Login was refused", 10, 11));
            s.expect(ConnectionMethods.CloseOk.class);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            AuthenticationFailureException a = (AuthenticationFailureException) unwrap(e);
            assertEquals(403, a.replyCode());
            assertTrue(a.getMessage().contains("ACCESS_REFUSED"));
        }
    }

    @Test
    void eofAfterStartOkIsAuthenticationFailure() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.send(0, new ConnectionMethods.Start(0, 9, Map.of(), "PLAIN", "en_US"));
            s.expect(ConnectionMethods.StartOk.class);
            s.close();
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            assertTrue(unwrap(e) instanceof AuthenticationFailureException, unwrap(e).toString());
        }
    }

    @Test
    void close530AfterOpenIsConnectionClosed() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.send(0, new ConnectionMethods.Start(0, 9, Map.of(), "PLAIN", "en_US"));
            s.expect(ConnectionMethods.StartOk.class);
            s.send(0, new ConnectionMethods.Tune(0, 0, 0));
            s.expect(ConnectionMethods.TuneOk.class);
            s.expect(ConnectionMethods.Open.class);
            s.send(0, new ConnectionMethods.Close(530, "NOT_ALLOWED - vhost not found", 10, 40));
            s.expect(ConnectionMethods.CloseOk.class);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            ConnectionClosedException c = (ConnectionClosedException) unwrap(e);
            assertEquals(530, c.replyCode());
            assertTrue(c.replyText().contains("vhost not found"));
        }
    }

    @Test
    void serverWithoutPlainIsRejected() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.send(0, new ConnectionMethods.Start(0, 9, Map.of(), "AMQPLAIN EXTERNAL", "en_US"));
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            Throwable t = unwrap(e);
            assertTrue(t instanceof AmqpException, t.toString());
            assertTrue(t.getMessage().contains("PLAIN"));
        }
    }

    @Test
    void serverRepliesWithProtocolHeader() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.sendRaw(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0});
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            assertTrue(unwrap(e) instanceof ProtocolException, unwrap(e).toString());
            assertTrue(unwrap(e).getMessage().contains("1.0.0"));
        }
    }

    @Test
    void tuneAboveClientRequestIsClampedAndZeroMeansServerValue() throws Exception {
        CompletableFuture<AmqpConnection> f = openAsync(config().requestedChannelMax(0).requestedFrameMax(0).build());
        try (FakeAmqpServer.Session s = server.accept()) {
            s.handshake(5, 65536, 0);
            AmqpConnection c = f.get(5, TimeUnit.SECONDS);
            assertEquals(5, c.channelMax());
            assertEquals(65536, c.frameMax());
            assertEquals(0, c.heartbeatSeconds());
            c.abort();
        }
    }

    @Test
    void connectTimeoutToUnreachableAddress() {
        // 10.255.255.1 is a non-routable address; the connect must fail within the timeout rather than hang.
        ConnectionConfig cfg = ConnectionConfig.builder().host("10.255.255.1").port(5672).connectionTimeoutMs(300).build();
        long t0 = System.currentTimeMillis();
        assertThrows(IOException.class, () -> AmqpConnection.open(cfg));
        assertTrue(System.currentTimeMillis() - t0 < 5000);
        assertFalse(Thread.currentThread().isInterrupted());
        assertNotNull(FrameCodec.PROTOCOL_HEADER);
    }
}
