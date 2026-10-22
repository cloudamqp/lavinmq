package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ConnectionStartStopIT {
    @Test
    void noDeliveryBeforeStartAndStopBlocks() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("startstop"));
            session.createProducer(q).send(session.createTextMessage("1"));
            MessageConsumer c = session.createConsumer(q);
            assertNull(c.receive(300));
            conn.start();
            assertEquals("1", ((TextMessage) c.receive(5000)).getText());
            conn.stop();
            session.createProducer(q).send(session.createTextMessage("2"));
            assertNull(c.receive(300));
            conn.start();
            assertEquals("2", ((TextMessage) c.receive(5000)).getText());
        }
    }

    @Test
    void stopWaitsForListenerAndCannotBeCalledFromListener() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("stoplistener"));
            MessageConsumer c = session.createConsumer(q);
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Throwable> fromListener = new AtomicReference<>();
            c.setMessageListener(m -> {
                try {
                    conn.stop();
                } catch (Exception e) {
                    fromListener.set(e);
                }
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            conn.start();
            session.createProducer(q).send(session.createTextMessage("x"));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(fromListener.get() instanceof IllegalStateException, String.valueOf(fromListener.get()));
            Thread stopper = new Thread(() -> {
                try {
                    conn.stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            stopper.start();
            stopper.join(500);
            assertTrue(stopper.isAlive(), "stop() must block while onMessage runs");
            release.countDown();
            stopper.join(5000);
            assertTrue(!stopper.isAlive());
            assertNotNull(c.getMessageListener());
            assertThrows(IllegalStateException.class, () -> c.receive(10));
        }
    }
}
