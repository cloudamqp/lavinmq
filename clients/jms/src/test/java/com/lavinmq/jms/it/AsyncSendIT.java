package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.CompletionListener;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AsyncSendIT {
    @Test
    void completionListenersRunInOrder() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("async"));
            MessageProducer p = s.createProducer(q);
            int n = 100;
            CountDownLatch done = new CountDownLatch(n);
            List<String> order = new ArrayList<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            CompletionListener l = new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    synchronized (order) {
                        try {
                            order.add(message.getJMSMessageID());
                        } catch (Exception e) {
                            failure.set(e);
                        }
                    }
                    done.countDown();
                }

                @Override
                public void onException(Message message, Exception exception) {
                    failure.set(exception);
                    done.countDown();
                }
            };
            List<String> sent = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                Message m = s.createTextMessage("m" + i);
                p.send(m, l);
                sent.add(m.getJMSMessageID());
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
            assertNull(failure.get());
            assertEquals(sent, order);
            Jms.awaitQueueCounts(q.getQueueName(), n, 0);
            p.close();
        }
    }

    @Test
    void transactedAsyncSendCompletesOnPublish() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession(Session.SESSION_TRANSACTED);
            Queue q = s.createQueue(Jms.unique("asynctx"));
            CountDownLatch done = new CountDownLatch(1);
            s.createProducer(q).send(s.createTextMessage("x"), new CompletionListener() {
                @Override
                public void onCompletion(Message message) {
                    done.countDown();
                }

                @Override
                public void onException(Message message, Exception exception) {
                }
            });
            assertTrue(done.await(5, TimeUnit.SECONDS));
            s.commit();
            Jms.awaitQueueCounts(q.getQueueName(), 1, 0);
        }
    }
}
