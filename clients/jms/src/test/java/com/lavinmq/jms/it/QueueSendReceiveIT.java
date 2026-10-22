package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class QueueSendReceiveIT {
    private Connection conn;
    private Session session;

    @BeforeEach
    void setUp() throws Exception {
        conn = Jms.factory().createConnection();
        session = conn.createSession(Session.AUTO_ACKNOWLEDGE);
        conn.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        conn.close();
    }

    @Test
    void sendAndReceiveText() throws Exception {
        Queue q = session.createQueue(Jms.unique("q"));
        MessageProducer p = session.createProducer(q);
        MessageConsumer c = session.createConsumer(q);
        TextMessage sent = session.createTextMessage("hello");
        sent.setStringProperty("k", "v");
        sent.setIntProperty("n", 7);
        long before = System.currentTimeMillis();
        p.send(sent);
        assertNotNull(sent.getJMSMessageID());
        assertTrue(sent.getJMSMessageID().startsWith("ID:"));
        assertTrue(sent.getJMSTimestamp() >= before);
        assertEquals(q, sent.getJMSDestination());
        assertEquals(DeliveryMode.PERSISTENT, sent.getJMSDeliveryMode());
        assertEquals(4, sent.getJMSPriority());
        assertEquals(0, sent.getJMSExpiration());

        Message m = c.receive(5000);
        assertNotNull(m);
        assertTrue(m instanceof TextMessage);
        assertEquals("hello", ((TextMessage) m).getText());
        assertEquals(sent.getJMSMessageID(), m.getJMSMessageID());
        assertEquals(sent.getJMSTimestamp(), m.getJMSTimestamp());
        assertEquals(q, m.getJMSDestination());
        assertEquals("v", m.getStringProperty("k"));
        assertEquals(7, m.getIntProperty("n"));
        assertEquals(1, m.getIntProperty("JMSXDeliveryCount"));
        assertFalse(m.getJMSRedelivered());
        assertEquals(DeliveryMode.PERSISTENT, m.getJMSDeliveryMode());
        assertNull(c.receive(200));
        assertNull(c.receiveNoWait());
        Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
    }

    @Test
    void orderAndAutoAck() throws Exception {
        Queue q = session.createQueue(Jms.unique("order"));
        MessageProducer p = session.createProducer(q);
        for (int i = 0; i < 50; i++) p.send(session.createTextMessage("m" + i));
        MessageConsumer c = session.createConsumer(q);
        for (int i = 0; i < 50; i++) assertEquals("m" + i, ((TextMessage) c.receive(5000)).getText());
        Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
    }

    @Test
    void nonPersistentAndPriority() throws Exception {
        Queue q = session.createQueue(Jms.unique("np"));
        MessageProducer p = session.createProducer(q);
        p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        p.setPriority(8);
        p.send(session.createTextMessage("x"));
        MessageConsumer c = session.createConsumer(q);
        Message m = c.receive(5000);
        assertEquals(DeliveryMode.NON_PERSISTENT, m.getJMSDeliveryMode());
        assertEquals(8, m.getJMSPriority());
    }

    @Test
    void producerWithoutDestination() throws Exception {
        Queue q = session.createQueue(Jms.unique("anon"));
        MessageProducer p = session.createProducer(null);
        p.send(q, session.createTextMessage("x"));
        assertEquals("x", ((TextMessage) session.createConsumer(q).receive(5000)).getText());
    }

    @Test
    void preDeclaredQueueWithArgumentsIsUsedAsIs() throws Exception {
        String name = Jms.unique("prio");
        Jms.httpPut("/api/queues/" + Jms.vhostPath() + "/" + name, "{\"durable\":true,\"arguments\":{\"x-max-priority\":5}}");
        Queue q = session.createQueue(name);
        MessageProducer p = session.createProducer(q);
        p.send(session.createTextMessage("low"), DeliveryMode.PERSISTENT, 1, 0);
        p.send(session.createTextMessage("high"), DeliveryMode.PERSISTENT, 5, 0);
        Jms.awaitQueueCounts(name, 2, 0);
        MessageConsumer c = session.createConsumer(q);
        assertEquals("high", ((TextMessage) c.receive(5000)).getText());
        assertEquals("low", ((TextMessage) c.receive(5000)).getText());
    }

    @Test
    void disabledMessageIdAndTimestamp() throws Exception {
        Queue q = session.createQueue(Jms.unique("dis"));
        MessageProducer p = session.createProducer(q);
        p.setDisableMessageID(true);
        p.setDisableMessageTimestamp(true);
        TextMessage m = session.createTextMessage("x");
        p.send(m);
        assertNull(m.getJMSMessageID());
        assertEquals(0, m.getJMSTimestamp());
        Message r = session.createConsumer(q).receive(5000);
        assertNull(r.getJMSMessageID());
        assertEquals(0, r.getJMSTimestamp());
    }
}
