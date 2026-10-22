package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.Test;

class ClientAckIT {
    @Test
    void acknowledgeAcksEverythingConsumedInTheSession() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession(Session.CLIENT_ACKNOWLEDGE);
            Queue q = session.createQueue(Jms.unique("clientack"));
            MessageProducer p = session.createProducer(q);
            for (int i = 0; i < 5; i++) p.send(session.createTextMessage("m" + i));
            conn.start();
            MessageConsumer c = session.createConsumer(q);
            Message m0 = c.receive(5000);
            Message m1 = c.receive(5000);
            Message m2 = c.receive(5000);
            Jms.awaitQueueCounts(q.getQueueName(), 0, 5); // prefetch pulled everything in
            m1.acknowledge(); // acks m0, m1 and m2 (all consumed), not the buffered ones
            Jms.awaitQueueCounts(q.getQueueName(), 0, 2);
            Message m3 = c.receive(5000);
            Message m4 = c.receive(5000);
            assertEquals("m4", ((TextMessage) m4).getText());
            m3.acknowledge();
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
            assertTrue(m0.getJMSMessageID() != null && m2.getJMSMessageID() != null);
        }
    }

    @Test
    void recoverRedeliversUnacknowledged() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession(Session.CLIENT_ACKNOWLEDGE);
            Queue q = session.createQueue(Jms.unique("recover"));
            MessageProducer p = session.createProducer(q);
            p.send(session.createTextMessage("a"));
            p.send(session.createTextMessage("b"));
            conn.start();
            MessageConsumer c = session.createConsumer(q);
            Message a = c.receive(5000);
            assertEquals("a", ((TextMessage) a).getText());
            session.recover();
            Message again = c.receive(5000);
            assertEquals("a", ((TextMessage) again).getText());
            assertTrue(again.getJMSRedelivered());
            Message b = c.receive(5000);
            assertEquals("b", ((TextMessage) b).getText());
            b.acknowledge();
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
            assertNull(c.receive(200));
        }
    }

    @Test
    void unackedMessagesReturnToQueueOnClose() throws Exception {
        Queue q;
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession(Session.CLIENT_ACKNOWLEDGE);
            q = session.createQueue(Jms.unique("unacked"));
            session.createProducer(q).send(session.createTextMessage("x"));
            conn.start();
            assertEquals("x", ((TextMessage) session.createConsumer(q).receive(5000)).getText());
        }
        Jms.awaitQueueCounts(q.getQueueName(), 1, 0);
    }

    @Test
    void dupsOkAcksInBatches() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession(Session.DUPS_OK_ACKNOWLEDGE);
            Queue q = session.createQueue(Jms.unique("dupsok"));
            MessageProducer p = session.createProducer(q);
            for (int i = 0; i < 25; i++) p.send(session.createTextMessage("m" + i));
            conn.start();
            MessageConsumer c = session.createConsumer(q);
            for (int i = 0; i < 9; i++) c.receive(5000);
            Jms.awaitQueueCounts(q.getQueueName(), 0, 25);
            c.receive(5000); // 10th consumed message triggers the batch ack
            Jms.awaitQueueCounts(q.getQueueName(), 0, 15);
            for (int i = 0; i < 15; i++) c.receive(5000);
            Jms.awaitQueueCounts(q.getQueueName(), 0, 5);
            session.close(); // channel close requeues the 5 unacked ones
            Jms.awaitQueueCounts(q.getQueueName(), 5, 0);
        }
    }
}
