package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class TemporaryDestinationIT {
    @Test
    void temporaryQueueLifecycle() throws Exception {
        String name;
        try (Connection conn = Jms.factory().createConnection(); Connection other = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            TemporaryQueue tq = s.createTemporaryQueue();
            name = tq.getQueueName();
            assertTrue(name.startsWith("amq.gen-"), name);
            conn.start();
            // another connection may send to it but not consume from it
            Session so = other.createSession();
            so.createProducer(so.createQueue(name)).send(so.createTextMessage("hi"));
            assertThrows(InvalidDestinationException.class, () -> so.createConsumer(so.createQueue(name)));
            MessageConsumer c = s.createConsumer(tq);
            assertEquals("hi", ((TextMessage) c.receive(5000)).getText());
            assertThrows(JMSException.class, tq::delete); // still has a consumer
            c.close();
            tq.delete();
            assertThrows(IOException.class, () -> Jms.queueJson(name));
        }
    }

    @Test
    void temporaryQueueDiesWithConnection() throws Exception {
        String name;
        try (Connection conn = Jms.factory().createConnection()) {
            name = conn.createSession().createTemporaryQueue().getQueueName();
            Jms.queueJson(name);
        }
        Thread.sleep(200);
        assertThrows(IOException.class, () -> Jms.queueJson(name));
    }

    @Test
    void temporaryTopic() throws Exception {
        try (Connection conn = Jms.factory().createConnection(); Connection other = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            TemporaryTopic tt = s.createTemporaryTopic();
            MessageConsumer c = s.createConsumer(tt);
            conn.start();
            Session so = other.createSession();
            so.createProducer(so.createTopic(tt.getTopicName())).send(so.createTextMessage("to temp topic"));
            assertEquals("to temp topic", ((TextMessage) c.receive(5000)).getText());
            assertThrows(InvalidDestinationException.class, () -> so.createConsumer(so.createTopic(tt.getTopicName())));
            assertThrows(JMSException.class, tt::delete);
            c.close();
            tt.delete();
        }
    }
}
