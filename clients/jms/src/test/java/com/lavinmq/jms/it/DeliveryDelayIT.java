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
import jakarta.jms.Topic;
import org.junit.jupiter.api.Test;

class DeliveryDelayIT {
    @Test
    void delayedQueueAndTopicSends() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("delayq"));
            Topic t = s.createTopic(Jms.unique("delayt"));
            MessageConsumer cq = s.createConsumer(q);
            MessageConsumer ct = s.createConsumer(t);
            conn.start();
            MessageProducer p = s.createProducer(null);
            p.setDeliveryDelay(1000);
            long t0 = System.currentTimeMillis();
            TextMessage mq = s.createTextMessage("delayed queue");
            p.send(q, mq);
            p.send(t, s.createTextMessage("delayed topic"));
            assertTrue(mq.getJMSDeliveryTime() >= t0 + 1000);
            assertNull(cq.receive(500));
            Message rq = cq.receive(5000);
            long elapsed = System.currentTimeMillis() - t0;
            assertTrue(elapsed >= 950, "delivered after " + elapsed + " ms");
            assertEquals("delayed queue", ((TextMessage) rq).getText());
            assertEquals(q, rq.getJMSDestination());
            assertEquals(mq.getJMSDeliveryTime(), rq.getJMSDeliveryTime());
            assertNull(rq.getObjectProperty("x-delay"));
            Message rt = ct.receive(5000);
            assertEquals("delayed topic", ((TextMessage) rt).getText());
            assertEquals(t, rt.getJMSDestination());
            // without delay the same producer publishes directly
            p.setDeliveryDelay(0);
            p.send(q, s.createTextMessage("now"));
            assertEquals("now", ((TextMessage) cq.receive(5000)).getText());
        }
    }
}
