package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueueBrowserIT {
    @Test
    void browseLeavesMessagesInPlace() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("browse"));
            MessageProducer p = s.createProducer(q);
            for (int i = 0; i < 5; i++) p.send(s.createTextMessage("m" + i));
            Jms.awaitQueueCounts(q.getQueueName(), 5, 0);
            QueueBrowser b = s.createBrowser(q);
            Enumeration<?> e = b.getEnumeration();
            List<String> seen = new ArrayList<>();
            while (e.hasMoreElements()) seen.add(((TextMessage) e.nextElement()).getText());
            assertEquals(List.of("m0", "m1", "m2", "m3", "m4"), seen);
            assertFalse(e.hasMoreElements());
            Jms.awaitQueueCounts(q.getQueueName(), 0, 5);
            b.close();
            Jms.awaitQueueCounts(q.getQueueName(), 5, 0);
            conn.start();
            Message m = s.createConsumer(q).receive(5000);
            assertEquals("m0", ((TextMessage) m).getText());
            assertTrue(m.getJMSRedelivered());
        }
    }
}
