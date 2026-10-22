package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.LavinMQConnection;
import com.lavinmq.jms.LavinMQConnectionFactory;
import com.lavinmq.jms.RecoveryListener;
import com.lavinmq.jms.amqp.it.Broker;
import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TransactionRolledBackException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AutoRecoveryIT {
    private TcpProxy proxy;
    private final BlockingQueue<String> events = new LinkedBlockingQueue<>();

    @BeforeEach
    void startProxy() throws Exception {
        proxy = new TcpProxy(Broker.URI.host(), Broker.URI.port());
    }

    @AfterEach
    void stopProxy() throws Exception {
        proxy.close();
    }

    private LavinMQConnectionFactory factory() {
        LavinMQConnectionFactory f = Jms.factory();
        f.setHost("127.0.0.1");
        f.setPort(proxy.port());
        f.setRecoveryInterval(200);
        f.setRecoveryMaxInterval(500);
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
        return f;
    }

    private void awaitEvent(String expected) throws InterruptedException {
        String e = events.poll(15, TimeUnit.SECONDS);
        assertEquals(expected, e);
    }

    @Test
    void listenerConsumerKeepsReceivingAfterDrop() throws Exception {
        try (Connection conn = factory().createConnection()) {
            BlockingQueue<JMSException> exceptions = new LinkedBlockingQueue<>();
            conn.setExceptionListener(exceptions::add);
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("rec-listener"));
            MessageConsumer c = s.createConsumer(q);
            BlockingQueue<Message> received = new LinkedBlockingQueue<>();
            CountDownLatch inFlight = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            c.setMessageListener(m -> {
                received.add(m);
                if (received.size() == 1) {
                    inFlight.countDown();
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            conn.start();
            MessageProducer p = s.createProducer(q);
            p.send(s.createTextMessage("one"));
            assertTrue(inFlight.await(5, TimeUnit.SECONDS));
            assertFalse(received.poll(1, TimeUnit.SECONDS).getJMSRedelivered());
            proxy.dropAll(); // while "one" is unacked in the listener
            awaitEvent("started");
            JMSException notice = exceptions.poll(5, TimeUnit.SECONDS);
            assertNotNull(notice);
            assertEquals("RECOVERING", notice.getErrorCode());
            release.countDown();
            awaitEvent("recovered");
            // "one" was requeued by the broker when the old connection died and comes back redelivered
            Message again = received.poll(10, TimeUnit.SECONDS);
            assertNotNull(again);
            assertEquals("one", ((TextMessage) again).getText());
            assertTrue(again.getJMSRedelivered());
            p.send(s.createTextMessage("two"));
            assertEquals("two", ((TextMessage) received.poll(10, TimeUnit.SECONDS)).getText());
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
            assertEquals(2, proxy.accepted());
        }
    }

    @Test
    void blockedReceiveAndSendSurviveAnOutage() throws Exception {
        try (Connection conn = factory().createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("rec-sync"));
            MessageConsumer c = s.createConsumer(q);
            MessageProducer p = s.createProducer(q);
            conn.start();
            CompletableFuture<Message> blocked = CompletableFuture.supplyAsync(() -> {
                try {
                    return c.receive(20_000);
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(200);
            proxy.refuse();
            proxy.dropAll();
            awaitEvent("started");
            // a send during the outage blocks until the connection is back
            CompletableFuture<Void> send = CompletableFuture.runAsync(() -> {
                try {
                    p.send(s.createTextMessage("after outage"));
                } catch (JMSException e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(700);
            assertFalse(send.isDone());
            proxy.resume();
            awaitEvent("recovered");
            send.get(10, TimeUnit.SECONDS);
            assertEquals("after outage", ((TextMessage) blocked.get(10, TimeUnit.SECONDS)).getText());
        }
    }

    @Test
    void subscriptionsAndTemporaryQueuesAreRestored() throws Exception {
        LavinMQConnectionFactory f = factory();
        f.setClientID(Jms.unique("client"));
        try (Connection conn = f.createConnection()) {
            Session s = conn.createSession();
            Topic t = s.createTopic(Jms.unique("rec-topic"));
            MessageConsumer plain = s.createConsumer(t);
            MessageConsumer shared = s.createSharedConsumer(t, "shared");
            MessageConsumer durable = s.createDurableConsumer(t, "durable");
            TemporaryQueue tq = s.createTemporaryQueue();
            String oldTempName = tq.getQueueName();
            MessageConsumer temp = s.createConsumer(tq);
            conn.start();
            proxy.dropAll();
            awaitEvent("started");
            awaitEvent("recovered");
            assertNotEquals(oldTempName, tq.getQueueName());
            MessageProducer p = s.createProducer(null);
            p.send(t, s.createTextMessage("topic"));
            p.send(tq, s.createTextMessage("temp"));
            assertEquals("topic", ((TextMessage) plain.receive(5000)).getText());
            assertEquals("topic", ((TextMessage) shared.receive(5000)).getText());
            assertEquals("topic", ((TextMessage) durable.receive(5000)).getText());
            assertEquals("temp", ((TextMessage) temp.receive(5000)).getText());
            durable.close();
            s.unsubscribe("durable");
        }
    }

    @Test
    void transactionLostDuringOutageIsReported() throws Exception {
        try (Connection conn = factory().createConnection()) {
            Session tx = conn.createSession(Session.SESSION_TRANSACTED);
            Queue q = tx.createQueue(Jms.unique("rec-tx"));
            MessageProducer p = tx.createProducer(q);
            p.send(tx.createTextMessage("uncommitted"));
            proxy.dropAll();
            awaitEvent("started");
            awaitEvent("recovered");
            assertThrows(TransactionRolledBackException.class, tx::commit);
            p.send(tx.createTextMessage("committed"));
            tx.commit();
            Jms.awaitQueueCounts(q.getQueueName(), 1, 0);
        }
    }

    @Test
    void clientAckOfMessageFromBeforeTheDropFails() throws Exception {
        try (Connection conn = factory().createConnection()) {
            Session s = conn.createSession(Session.CLIENT_ACKNOWLEDGE);
            Queue q = s.createQueue(Jms.unique("rec-clientack"));
            s.createProducer(q).send(s.createTextMessage("x"));
            conn.start();
            MessageConsumer c = s.createConsumer(q);
            Message m = c.receive(5000);
            proxy.dropAll();
            awaitEvent("started");
            awaitEvent("recovered");
            assertThrows(IllegalStateException.class, m::acknowledge);
            Message again = c.receive(5000);
            assertTrue(again.getJMSRedelivered());
            again.acknowledge();
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
        }
    }

    @Test
    void givesUpAfterConfiguredAttempts() throws Exception {
        LavinMQConnectionFactory f = factory();
        f.setRecoveryAttempts(2);
        f.setRecoveryTimeout(3000);
        Connection conn = f.createConnection();
        AtomicReference<JMSException> last = new AtomicReference<>();
        CountDownLatch twoExceptions = new CountDownLatch(2);
        conn.setExceptionListener(e -> {
            last.set(e);
            twoExceptions.countDown();
        });
        Session s = conn.createSession();
        Queue q = s.createQueue(Jms.unique("rec-giveup"));
        MessageProducer p = s.createProducer(q);
        conn.start();
        proxy.refuse();
        proxy.dropAll();
        awaitEvent("started");
        awaitEvent("failed");
        assertTrue(twoExceptions.await(5, TimeUnit.SECONDS));
        assertNotEquals("RECOVERING", last.get().getErrorCode());
        assertTrue(((LavinMQConnection) conn).isClosed());
        assertThrows(IllegalStateException.class, () -> p.send(s.createTextMessage("x")));
        assertThrows(IllegalStateException.class, conn::createSession);
        conn.close();
    }

    @Test
    void recoveryCanBeDisabled() throws Exception {
        LavinMQConnectionFactory f = factory();
        f.setAutomaticRecovery(false);
        Connection conn = f.createConnection();
        CountDownLatch notified = new CountDownLatch(1);
        AtomicReference<JMSException> err = new AtomicReference<>();
        conn.setExceptionListener(e -> {
            err.set(e);
            notified.countDown();
        });
        Session s = conn.createSession();
        Queue q = s.createQueue(Jms.unique("rec-off"));
        MessageConsumer c = s.createConsumer(q);
        conn.start();
        CompletableFuture<Throwable> blocked = CompletableFuture.supplyAsync(() -> {
            try {
                c.receive(10_000);
                return null;
            } catch (JMSException e) {
                return e;
            }
        });
        Thread.sleep(200);
        proxy.dropAll();
        assertTrue(notified.await(5, TimeUnit.SECONDS));
        assertNotNull(err.get());
        assertTrue(blocked.get(5, TimeUnit.SECONDS) instanceof IllegalStateException);
        assertTrue(((LavinMQConnection) conn).isClosed());
        assertNull(events.poll(200, TimeUnit.MILLISECONDS));
        conn.close();
    }

    @Test
    void serverInitiatedCloseTriggersRecovery() throws Exception {
        LavinMQConnectionFactory f = factory();
        String suffix = Jms.unique("kill-me");
        f.setConnectionNameSuffix(suffix);
        try (Connection conn = f.createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("rec-server"));
            MessageConsumer c = s.createConsumer(q);
            conn.start();
            String name = null;
            for (int i = 0; i < 50 && name == null; i++) {
                String json = Jms.httpGet("/api/connections");
                int at = json.indexOf(suffix);
                if (at >= 0) {
                    java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"").matcher(json);
                    if (m.find(at)) name = m.group(1);
                }
                if (name == null) Thread.sleep(100);
            }
            assertNotNull(name, "connection not found in the HTTP API");
            int status = Jms.httpDelete("/api/connections/" + java.net.URLEncoder.encode(name, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20"));
            assertTrue(status == 204 || status == 200, "status " + status);
            awaitEvent("started");
            awaitEvent("recovered");
            s.createProducer(q).send(s.createTextMessage("after kill"));
            assertEquals("after kill", ((TextMessage) c.receive(5000)).getText());
        }
    }
}
