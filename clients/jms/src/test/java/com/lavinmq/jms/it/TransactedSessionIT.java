package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.Test;

class TransactedSessionIT {
    @Test
    void commitAndRollbackOfSends() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session tx = conn.createSession(Session.SESSION_TRANSACTED);
            Session plain = conn.createSession();
            Queue q = tx.createQueue(Jms.unique("tx"));
            MessageProducer p = tx.createProducer(q);
            conn.start();
            MessageConsumer c = plain.createConsumer(q);
            p.send(tx.createTextMessage("rolled back"));
            tx.rollback();
            assertNull(c.receive(300));
            p.send(tx.createTextMessage("a"));
            p.send(tx.createTextMessage("b"));
            assertNull(c.receive(300));
            tx.commit();
            assertEquals("a", ((TextMessage) c.receive(5000)).getText());
            assertEquals("b", ((TextMessage) c.receive(5000)).getText());
            assertTrue(tx.getTransacted());
            assertThrows(IllegalStateException.class, tx::recover);
            assertThrows(IllegalStateException.class, plain::commit);
        }
    }

    @Test
    void rollbackRedeliversConsumedMessages() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session tx = conn.createSession(Session.SESSION_TRANSACTED);
            Queue q = tx.createQueue(Jms.unique("txrecv"));
            MessageProducer p = tx.createProducer(q);
            p.send(tx.createTextMessage("x"));
            tx.commit();
            conn.start();
            MessageConsumer c = tx.createConsumer(q);
            Message first = c.receive(5000);
            assertEquals("x", ((TextMessage) first).getText());
            tx.rollback();
            Message again = c.receive(5000);
            assertTrue(again.getJMSRedelivered());
            tx.commit();
            Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
            assertNull(c.receive(200));
        }
    }

    @Test
    void consumeAndProduceAtomically() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session tx = conn.createSession(Session.SESSION_TRANSACTED);
            Queue in = tx.createQueue(Jms.unique("in"));
            Queue out = tx.createQueue(Jms.unique("out"));
            tx.createProducer(in).send(tx.createTextMessage("job"));
            tx.commit();
            conn.start();
            MessageConsumer c = tx.createConsumer(in);
            Message job = c.receive(5000);
            tx.createProducer(out).send(tx.createTextMessage("result of " + ((TextMessage) job).getText()));
            Jms.awaitQueueCounts(out.getQueueName(), 0, 0);
            tx.commit();
            Jms.awaitQueueCounts(in.getQueueName(), 0, 0);
            Jms.awaitQueueCounts(out.getQueueName(), 1, 0);
        }
    }

    @Test
    void closeDiscardsOpenTransaction() throws Exception {
        Queue q;
        try (Connection conn = Jms.factory().createConnection()) {
            Session tx = conn.createSession(Session.SESSION_TRANSACTED);
            q = tx.createQueue(Jms.unique("txclose"));
            tx.createProducer(q).send(tx.createTextMessage("never"));
        }
        Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
    }
}
