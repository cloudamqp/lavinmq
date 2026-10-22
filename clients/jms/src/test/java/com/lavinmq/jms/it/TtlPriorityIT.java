package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Test;

class TtlPriorityIT {
    @Test
    void expiredMessagesAreNotDelivered() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            Queue q = s.createQueue(Jms.unique("ttl"));
            MessageProducer p = s.createProducer(q);
            TextMessage shortLived = s.createTextMessage("short");
            p.send(shortLived, DeliveryMode.PERSISTENT, 4, 300);
            assertTrue(shortLived.getJMSExpiration() > System.currentTimeMillis());
            p.send(s.createTextMessage("long"), DeliveryMode.PERSISTENT, 4, 60_000);
            Thread.sleep(700);
            conn.start();
            MessageConsumer c = s.createConsumer(q);
            Message m = c.receive(5000);
            assertEquals("long", ((TextMessage) m).getText());
            assertTrue(m.getJMSExpiration() > System.currentTimeMillis());
            assertNull(c.receive(200));
        }
    }

    @Test
    void priorityOrderOnPriorityQueue() throws Exception {
        try (Connection conn = Jms.factory().createConnection()) {
            Session s = conn.createSession();
            String name = Jms.unique("prioq");
            Jms.httpPut("/api/queues/" + Jms.vhostPath() + "/" + name, "{\"durable\":true,\"arguments\":{\"x-max-priority\":9}}");
            Queue q = s.createQueue(name);
            MessageProducer p = s.createProducer(q);
            for (int prio : new int[] {1, 9, 5}) p.send(s.createTextMessage("p" + prio), DeliveryMode.PERSISTENT, prio, 0);
            Jms.awaitQueueCounts(name, 3, 0);
            conn.start();
            MessageConsumer c = s.createConsumer(q);
            assertEquals("p9", ((TextMessage) c.receive(5000)).getText());
            assertEquals("p5", ((TextMessage) c.receive(5000)).getText());
            assertEquals("p1", ((TextMessage) c.receive(5000)).getText());
        }
    }
}
