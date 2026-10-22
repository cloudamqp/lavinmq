package com.lavinmq.jms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.method.BasicMethods;
import com.lavinmq.jms.amqp.method.ChannelMethods;
import com.lavinmq.jms.amqp.method.ConfirmMethods;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import com.lavinmq.jms.amqp.transport.FakeAmqpServer;
import jakarta.jms.Connection;
import jakarta.jms.Session;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Drives recovery against scripted servers to check address fail-over and session restoration. */
class RecoveryManagerTest {
    /** Answers the frames a JMS session sends when it (re)opens: channel.open, basic.qos, confirm.select. */
    private static void serveSession(FakeAmqpServer.Session s) throws Exception {
        int ch = s.acceptChannel();
        s.expect(BasicMethods.Qos.class);
        s.send(ch, new BasicMethods.QosOk());
        s.expect(ConfirmMethods.Select.class);
        s.send(ch, new ConfirmMethods.SelectOk());
    }

    @Test
    void failsOverToTheNextAddressAndRestoresTheSession() throws Exception {
        try (FakeAmqpServer a = new FakeAmqpServer(); FakeAmqpServer b = new FakeAmqpServer()) {
            LavinMQConnectionFactory f = new LavinMQConnectionFactory();
            f.setAddresses("127.0.0.1:" + a.port() + ",127.0.0.1:" + b.port());
            f.setHeartbeat(0);
            f.setRecoveryInterval(50);
            f.setRecoveryMaxInterval(100);
            f.setConnectionNameSuffix("failover-test");
            BlockingQueue<String> events = new LinkedBlockingQueue<>();
            f.setRecoveryListener(new RecoveryListener() {
                @Override
                public void onRecoveryStarted(Throwable cause) {
                    events.add("started");
                }

                @Override
                public void onRecovered() {
                    events.add("recovered");
                }

                @Override
                public void onRecoveryFailed(Throwable cause) {
                    events.add("failed");
                }
            });
            CompletableFuture<Connection> connecting = CompletableFuture.supplyAsync(() -> {
                try {
                    Connection c = f.createConnection();
                    c.createSession(Session.AUTO_ACKNOWLEDGE);
                    return c;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            FakeAmqpServer.Session sa = a.accept();
            ConnectionMethods.StartOk startOk = sa.handshake();
            assertTrue(startOk.clientProperties().get("connection_name").toString().endsWith("failover-test"));
            serveSession(sa);
            Connection conn = connecting.get(5, TimeUnit.SECONDS);

            // server A goes away: recovery must try B next, not A again
            sa.close();
            assertEquals("started", events.poll(5, TimeUnit.SECONDS));
            FakeAmqpServer.Session sb = b.accept();
            ConnectionMethods.StartOk again = sb.handshake();
            assertEquals(startOk.clientProperties().get("connection_name"), again.clientProperties().get("connection_name"));
            serveSession(sb);
            assertEquals("recovered", events.poll(5, TimeUnit.SECONDS));
            assertFalse(((LavinMQConnection) conn).isClosed());

            CompletableFuture<Void> closing = CompletableFuture.runAsync(() -> {
                try {
                    conn.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            FakeAmqpServer.Received r = sb.expect(ChannelMethods.Close.class);
            sb.send(r.channel(), new ChannelMethods.CloseOk());
            sb.acceptClose();
            closing.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void closingDuringRecoveryStopsTheLoop() throws Exception {
        try (FakeAmqpServer a = new FakeAmqpServer()) {
            LavinMQConnectionFactory f = new LavinMQConnectionFactory();
            f.setHost("127.0.0.1");
            f.setPort(a.port());
            f.setHeartbeat(0);
            f.setRecoveryInterval(100);
            BlockingQueue<String> events = new LinkedBlockingQueue<>();
            f.setRecoveryListener(new RecoveryListener() {
                @Override
                public void onRecoveryStarted(Throwable cause) {
                    events.add("started");
                }

                @Override
                public void onRecoveryFailed(Throwable cause) {
                    events.add("failed");
                }
            });
            CompletableFuture<Connection> connecting = CompletableFuture.supplyAsync(() -> {
                try {
                    return f.createConnection();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            FakeAmqpServer.Session sa = a.accept();
            sa.handshake();
            Connection conn = connecting.get(5, TimeUnit.SECONDS);
            sa.close();
            assertEquals("started", events.poll(5, TimeUnit.SECONDS));
            // the server keeps refusing; closing the connection must end recovery without reporting a failure
            FakeAmqpServer.Session retry = a.accept();
            retry.close();
            conn.close();
            assertTrue(((LavinMQConnection) conn).isClosed());
            assertEquals(null, events.poll(500, TimeUnit.MILLISECONDS));
        }
    }
}
