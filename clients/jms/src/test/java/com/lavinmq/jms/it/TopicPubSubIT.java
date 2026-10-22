package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.Test;

class TopicPubSubIT {
    @Test
    void everySubscriberGetsEveryMessageAndWildcardsWork() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session session = conn.createSession();
            String base = Jms.unique("news");
            Topic eu = session.createTopic(base + ".eu");
            Topic us = session.createTopic(base + ".us");
            Topic all = session.createTopic(base + ".*");
            MessageConsumer c1 = session.createConsumer(eu);
            MessageConsumer c2 = session.createConsumer(eu);
            MessageConsumer cAll = session.createConsumer(all);
            conn.start();
            MessageProducer p = session.createProducer(null);
            p.send(eu, session.createTextMessage("eu-1"));
            p.send(us, session.createTextMessage("us-1"));
            assertEquals("eu-1", ((TextMessage) c1.receive(5000)).getText());
            assertEquals("eu-1", ((TextMessage) c2.receive(5000)).getText());
            Message m1 = cAll.receive(5000);
            Message m2 = cAll.receive(5000);
            assertNotNull(m1);
            assertNotNull(m2);
            assertEquals(eu, m1.getJMSDestination());
            assertEquals(us, m2.getJMSDestination());
            assertNull(c1.receive(200));
            // a message published before any subscriber exists is dropped (topic semantics)
            Topic nobody = session.createTopic(base + ".nobody");
            p.send(nobody, session.createTextMessage("lost"));
            MessageConsumer late = session.createConsumer(nobody);
            assertNull(late.receive(200));
            c1.close();
            p.send(eu, session.createTextMessage("eu-2"));
            assertEquals("eu-2", ((TextMessage) c2.receive(5000)).getText());
        }
    }

    @Test
    void noLocalFiltersOwnMessages() throws Exception {
        try (Connection a = Jms.factory().createConnection(); Connection b = Jms.factory().createConnection()) {
            Session sa = a.createSession();
            Session sb = b.createSession();
            Topic t = sa.createTopic(Jms.unique("nolocal"));
            MessageConsumer ca = sa.createConsumer(t, null, true);
            a.start();
            sa.createProducer(t).send(sa.createTextMessage("from a"));
            sb.createProducer(t).send(sb.createTextMessage("from b"));
            Message m = ca.receive(5000);
            assertEquals("from b", ((TextMessage) m).getText());
            assertNull(ca.receive(300));
            assertTrue(((jakarta.jms.TopicSubscriber) ca).getNoLocal());
        }
    }
}
