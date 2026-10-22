package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.context.LavinMQJMSContext;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JMSContextIT {
    @Test
    void simplifiedApiEndToEnd() throws Exception {
        try (JMSContext ctx = Jms.factory().createContext()) {
            Queue q = ctx.createQueue(Jms.unique("ctx"));
            JMSProducer p = ctx.createProducer().setProperty("k", "v").setPriority(2).setJMSType("t").setTimeToLive(60_000);
            p.send(q, "text");
            p.send(q, Map.of("a", 1));
            p.send(q, new byte[] {1, 2});
            p.send(q, (java.io.Serializable) Integer.valueOf(42));
            JMSConsumer c = ctx.createConsumer(q); // auto-starts the connection
            Message m = c.receive(5000);
            assertEquals("text", ((TextMessage) m).getText());
            assertEquals("v", m.getStringProperty("k"));
            assertEquals(2, m.getJMSPriority());
            assertEquals("t", m.getJMSType());
            assertTrue(m.getJMSExpiration() > 0);
            assertEquals(1, c.receiveBody(Map.class, 5000).get("a"));
            assertEquals(2, c.receiveBody(byte[].class, 5000).length);
            assertEquals(42, c.receiveBody(Integer.class, 5000));
            assertNull(c.receiveBodyNoWait(String.class));
            assertThrows(InvalidSelectorRuntimeException.class, () -> ctx.createConsumer(q, "a = 1"));
            assertEquals(JMSContext.AUTO_ACKNOWLEDGE, ctx.getSessionMode());
        }
    }

    @Test
    void childContextsShareTheConnection() throws Exception {
        JMSContext parent = Jms.factory().createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        JMSContext child = parent.createContext(JMSContext.SESSION_TRANSACTED);
        assertEquals(((LavinMQJMSContext) parent).connection(), ((LavinMQJMSContext) child).connection());
        Queue q = parent.createQueue(Jms.unique("ctxshared"));
        child.createProducer().send(q, "tx");
        child.commit();
        JMSConsumer c = parent.createConsumer(q);
        Message m = c.receive(5000);
        assertNotNull(m);
        Jms.awaitQueueCounts(q.getQueueName(), 0, 1);
        parent.acknowledge();
        Jms.awaitQueueCounts(q.getQueueName(), 0, 0);
        parent.close();
        assertTrue(!((LavinMQJMSContext) child).connection().isClosed());
        child.close();
        assertTrue(((LavinMQJMSContext) child).connection().isClosed());
        assertThrows(IllegalStateRuntimeException.class, () -> child.createQueue("x"));
    }

    @Test
    void receiveBodyTypeMismatchKeepsMessageUnacked() throws Exception {
        try (JMSContext ctx = Jms.factory().createContext(JMSContext.CLIENT_ACKNOWLEDGE)) {
            Queue q = ctx.createQueue(Jms.unique("mismatch"));
            ctx.createProducer().send(q, "not a map");
            JMSConsumer c = ctx.createConsumer(q);
            assertThrows(MessageFormatRuntimeException.class, () -> c.receiveBody(Map.class, 5000));
            Jms.awaitQueueCounts(q.getQueueName(), 0, 1);
        }
    }
}
