package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.LavinMQConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.Test;

class DurableSubscriberIT {
    private static LavinMQConnectionFactory factory(String clientId) {
        LavinMQConnectionFactory f = Jms.factory();
        f.setClientID(clientId);
        return f;
    }

    @Test
    void survivesReconnectAndRejectsSecondActiveSubscriber() throws Exception {
        String clientId = Jms.unique("client");
        String sub = "orders";
        String topicName = Jms.unique("durable");
        try (Connection conn = factory(clientId).createConnection()) {
            Session s = conn.createSession();
            Topic t = s.createTopic(topicName);
            MessageConsumer c = s.createDurableConsumer(t, sub);
            conn.start();
            s.createProducer(t).send(s.createTextMessage("while connected"));
            assertEquals("while connected", ((TextMessage) c.receive(5000)).getText());
            // a second active subscriber on the same subscription is refused
            try (Connection other = factory(clientId).createConnection()) {
                Session so = other.createSession();
                JMSException e = assertThrows(JMSException.class, () -> so.createDurableConsumer(so.createTopic(topicName), sub));
                assertTrue(e.getMessage().contains("active consumer"), e.getMessage());
                // and the session is still usable afterwards
                so.createProducer(so.createTopic(topicName)).send(so.createTextMessage("while disconnected"));
            }
        }
        try (Connection conn = factory(clientId).createConnection()) {
            Session s = conn.createSession();
            MessageConsumer c = s.createDurableConsumer(s.createTopic(topicName), sub);
            conn.start();
            assertEquals("while disconnected", ((TextMessage) c.receive(5000)).getText());
            c.close();
            s.unsubscribe(sub);
            assertThrows(InvalidDestinationException.class, () -> s.unsubscribe(sub));
        }
    }

    @Test
    void topicChangeRecreatesTheSubscription() throws Exception {
        String clientId = Jms.unique("client");
        try (Connection conn = factory(clientId).createConnection()) {
            Session s = conn.createSession();
            Topic t1 = s.createTopic(Jms.unique("t1"));
            Topic t2 = s.createTopic(Jms.unique("t2"));
            MessageConsumer c1 = s.createDurableConsumer(t1, "sub");
            conn.start();
            s.createProducer(t1).send(s.createTextMessage("for t1"));
            assertEquals("for t1", ((TextMessage) c1.receive(5000)).getText());
            c1.close();
            s.createProducer(t1).send(s.createTextMessage("pending on t1"));
            MessageConsumer c2 = s.createDurableConsumer(t2, "sub");
            s.createProducer(t2).send(s.createTextMessage("for t2"));
            assertEquals("for t2", ((TextMessage) c2.receive(5000)).getText());
            assertNull(c2.receive(200));
            c2.close();
            s.unsubscribe("sub");
        }
    }

    @Test
    void unshareDurableNeedsClientIdAndUnsubscribeNeedsNoActiveConsumer() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            assertThrows(IllegalStateException.class, () -> s.createDurableConsumer(s.createTopic("x"), "sub"));
        }
        String clientId = Jms.unique("client");
        try (Connection conn = factory(clientId).createConnection()) {
            Session s = conn.createSession();
            MessageConsumer c = s.createDurableSubscriber(s.createTopic(Jms.unique("t")), "active");
            assertThrows(IllegalStateException.class, () -> s.unsubscribe("active"));
            c.close();
            s.unsubscribe("active");
        }
    }
}
