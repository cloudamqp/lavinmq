package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lavinmq.jms.LavinMQConnectionFactory;
import com.lavinmq.jms.PublisherConfirmMode;
import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import org.junit.jupiter.api.Test;

class PublisherConfirmIT {
    @Test
    void sendToDeletedQueueFailsWithInvalidDestination() throws Exception {
        LavinMQConnectionFactory f = Jms.factory();
        f.setPublisherConfirms(PublisherConfirmMode.ALWAYS);
        try (Connection conn = f.createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("deleted"));
            MessageProducer p = session.createProducer(q);
            p.send(session.createTextMessage("1"));
            assertEquals(204, Jms.httpDelete("/api/queues/" + Jms.vhostPath() + "/" + q.getQueueName()));
            InvalidDestinationException e = assertThrows(InvalidDestinationException.class, () -> p.send(session.createTextMessage("2")));
            assertEquals("312", e.getErrorCode());
            // the queue is re-created on the next send because the cache entry was evicted
            p.send(session.createTextMessage("3"));
            Jms.awaitQueueCounts(q.getQueueName(), 1, 0);
        }
    }

    @Test
    void manySendsWaitForConfirms() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            Queue q = session.createQueue(Jms.unique("confirms"));
            MessageProducer p = session.createProducer(q);
            for (int i = 0; i < 500; i++) p.send(session.createTextMessage("m" + i));
            Jms.awaitQueueCounts(q.getQueueName(), 500, 0);
        }
    }
}
