package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MessageListenerIT {
    @Test
    void serialDeliveryAcrossConsumersOfOneSession() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q1 = session.createQueue(Jms.unique("l1"));
            Queue q2 = session.createQueue(Jms.unique("l2"));
            MessageConsumer c1 = session.createConsumer(q1);
            MessageConsumer c2 = session.createConsumer(q2);
            AtomicInteger concurrent = new AtomicInteger();
            AtomicInteger maxConcurrent = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(200);
            List<String> threads = new CopyOnWriteArrayList<>();
            jakarta.jms.MessageListener l = m -> {
                int now = concurrent.incrementAndGet();
                maxConcurrent.accumulateAndGet(now, Math::max);
                threads.add(Thread.currentThread().getName());
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                concurrent.decrementAndGet();
                done.countDown();
            };
            c1.setMessageListener(l);
            c2.setMessageListener(l);
            conn.start();
            MessageProducer p = session.createProducer(null);
            for (int i = 0; i < 100; i++) {
                p.send(q1, session.createTextMessage("a" + i));
                p.send(q2, session.createTextMessage("b" + i));
            }
            assertTrue(done.await(20, TimeUnit.SECONDS));
            assertEquals(1, maxConcurrent.get());
            assertEquals(1, threads.stream().distinct().count());
            Jms.awaitQueueCounts(q1.getQueueName(), 0, 0);
            Jms.awaitQueueCounts(q2.getQueueName(), 0, 0);
        }
    }

    @Test
    void runtimeExceptionRedelivers() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("redeliver"));
            MessageConsumer c = session.createConsumer(q);
            BlockingQueue<Message> seen = new LinkedBlockingQueue<>();
            AtomicInteger calls = new AtomicInteger();
            c.setMessageListener(m -> {
                seen.add(m);
                if (calls.incrementAndGet() == 1) throw new RuntimeException("first attempt fails");
            });
            conn.start();
            session.createProducer(q).send(session.createTextMessage("x"));
            Message first = seen.poll(5, TimeUnit.SECONDS);
            Message second = seen.poll(5, TimeUnit.SECONDS);
            assertFalse(first.getJMSRedelivered());
            assertTrue(second.getJMSRedelivered());
            assertEquals(2, second.getIntProperty("JMSXDeliveryCount"));
            assertEquals("x", ((TextMessage) second).getText());
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
        }
    }

    @Test
    void sessionLevelListener() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("sessionlistener"));
            session.createConsumer(q);
            BlockingQueue<Message> seen = new LinkedBlockingQueue<>();
            session.setMessageListener(seen::add);
            conn.start();
            session.createProducer(q).send(session.createTextMessage("x"));
            assertEquals("x", ((TextMessage) seen.poll(5, TimeUnit.SECONDS)).getText());
        }
    }
}
