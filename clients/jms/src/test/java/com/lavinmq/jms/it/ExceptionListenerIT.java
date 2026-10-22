package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.LavinMQConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ExceptionListenerIT {
    private static LavinMQConnectionFactory factory(String clientId) {
        LavinMQConnectionFactory f = Jms.factory();
        f.setClientID(clientId);
        return f;
    }

    @Test
    void brokerClosedSessionChannelIsReportedAndSessionBecomesUnusable() throws Exception {
        String clientId = Jms.unique("client");
        String topicName = Jms.unique("excl");
        try (Connection holder = factory(clientId).createConnection(); Connection conn = factory(clientId).createConnection()) {
            Session hs = holder.createSession();
            hs.createDurableConsumer(hs.createTopic(topicName), "sub"); // keeps the subscription busy
            BlockingQueue<JMSException> errors = new LinkedBlockingQueue<>();
            conn.setExceptionListener(errors::add);
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("busy"));
            MessageConsumer active = s.createConsumer(q); // the session now has in-flight state, so its channel is not replaced
            conn.start();
            Topic t = s.createTopic(topicName);
            JMSException e = assertThrows(JMSException.class, () -> s.createDurableConsumer(t, "sub"));
            assertTrue(e.getMessage().contains("active consumer"), e.getMessage());
            JMSException reported = errors.poll(5, TimeUnit.SECONDS);
            assertNotNull(reported);
            assertEquals("403", reported.getErrorCode());
            assertThrows(IllegalStateException.class, () -> active.receive(100));
            assertThrows(IllegalStateException.class, s::createTextMessage);
            // other sessions on the connection are unaffected
            Session s2 = conn.createSession();
            s2.createProducer(q).send(s2.createTextMessage("still fine"));
            Jms.awaitQueueCounts(q.getQueueName(), 1, 0);
        }
    }
}
