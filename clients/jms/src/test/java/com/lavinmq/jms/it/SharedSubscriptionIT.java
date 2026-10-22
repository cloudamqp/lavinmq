package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class SharedSubscriptionIT {
    @Test
    void sharedConsumersSplitTheMessages() throws Exception {
        com.lavinmq.jms.LavinMQConnectionFactory f = Jms.factory();
        f.setPrefetch(1);
        try (Connection a = f.createConnection(); Connection b = f.createConnection()) {
            Session sa = a.createSession();
            Session sb = b.createSession();
            String topicName = Jms.unique("shared");
            String sub = Jms.unique("sub");
            MessageConsumer ca = sa.createSharedConsumer(sa.createTopic(topicName), sub);
            MessageConsumer cb = sb.createSharedConsumer(sb.createTopic(topicName), sub);
            a.start();
            b.start();
            Topic t = sa.createTopic(topicName);
            MessageProducer p = sa.createProducer(t);
            for (int i = 0; i < 20; i++) p.send(sa.createTextMessage("m" + i));
            Set<String> got = ConcurrentHashMap.newKeySet();
            int fromA = 0;
            int fromB = 0;
            for (int i = 0; i < 20; i++) {
                Message m = ca.receive(500);
                if (m != null) {
                    fromA++;
                    got.add(m.getJMSMessageID());
                    continue;
                }
                m = cb.receive(2000);
                if (m != null) {
                    fromB++;
                    got.add(m.getJMSMessageID());
                }
            }
            assertEquals(20, got.size());
            assertTrue(fromA > 0 && fromB > 0, "a=" + fromA + " b=" + fromB);
        }
    }

    @Test
    void sharedDurableSurvivesConsumers() throws Exception {
        String topicName = Jms.unique("shareddurable");
        String sub = Jms.unique("sub");
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            s.createSharedDurableConsumer(s.createTopic(topicName), sub).close();
            s.createProducer(s.createTopic(topicName)).send(s.createTextMessage("kept"));
        }
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            MessageConsumer c = s.createSharedDurableConsumer(s.createTopic(topicName), sub);
            conn.start();
            assertEquals("kept", ((jakarta.jms.TextMessage) c.receive(5000)).getText());
            c.close();
            s.unsubscribe(sub);
        }
    }
}
