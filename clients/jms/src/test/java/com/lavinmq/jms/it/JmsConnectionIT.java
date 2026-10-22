package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.LavinMQConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class JmsConnectionIT {
    @Test
    void connectAndMetaData() throws Exception {
        try (Connection c = Jms.factory().createConnection()) {
            ConnectionMetaData md = c.getMetaData();
            assertEquals("LavinMQ", md.getJMSProviderName());
            assertEquals(3, md.getJMSMajorVersion());
            assertEquals(1, md.getJMSMinorVersion());
            assertTrue(md.getProviderMajorVersion() >= 2, md.getProviderVersion());
            assertTrue(Collections.list(md.getJMSXPropertyNames()).contains("JMSXDeliveryCount"));
            assertNull(c.getClientID());
            Session s = c.createSession(Session.AUTO_ACKNOWLEDGE);
            assertEquals(Session.AUTO_ACKNOWLEDGE, s.getAcknowledgeMode());
            s.close();
            assertThrows(IllegalStateException.class, () -> s.createQueue("x"));
        }
    }

    @Test
    void clientIdRules() throws Exception {
        try (Connection c = Jms.factory().createConnection()) {
            c.setClientID("app-1");
            assertEquals("app-1", c.getClientID());
            assertThrows(IllegalStateException.class, () -> c.setClientID("app-2"));
        }
        try (Connection c = Jms.factory().createConnection()) {
            c.createSession();
            assertThrows(IllegalStateException.class, () -> c.setClientID("late"));
        }
        LavinMQConnectionFactory f = Jms.factory();
        f.setClientID("from-factory");
        try (Connection c = f.createConnection()) {
            assertEquals("from-factory", c.getClientID());
        }
    }

    @Test
    void badCredentials() {
        LavinMQConnectionFactory f = Jms.factory();
        JMSSecurityException e = assertThrows(JMSSecurityException.class, () -> f.createConnection("guest", "wrong"));
        assertEquals("403", e.getErrorCode());
        assertThrows(jakarta.jms.JMSSecurityRuntimeException.class, () -> f.createContext("guest", "wrong"));
    }

    @Test
    void autoDeclareQueuesCanBeDisabled() throws Exception {
        LavinMQConnectionFactory f = Jms.factory();
        f.setAutoDeclareQueues(false);
        try (Connection c = f.createConnection()) {
            Session s = c.createSession();
            Queue q = s.createQueue(Jms.unique("missing"));
            InvalidDestinationException e = assertThrows(InvalidDestinationException.class, () -> s.createProducer(q).send(s.createTextMessage("x")));
            assertEquals("404", e.getErrorCode());
            // the session survives a failed declaration
            Queue existing = s.createQueue(Jms.unique("existing"));
            Jms.httpPut("/api/queues/" + Jms.vhostPath() + "/" + existing.getQueueName(), "{\"durable\":true}");
            s.createProducer(existing).send(s.createTextMessage("ok"));
            Jms.awaitQueueCounts(existing.getQueueName(), 1, 0);
        }
    }

    @Test
    void selectorsAreRejected() throws Exception {
        try (Connection c = Jms.factory().createConnection()) {
            Session s = c.createSession();
            Queue q = s.createQueue(Jms.unique("sel"));
            assertThrows(InvalidSelectorException.class, () -> s.createConsumer(q, "x = 1"));
            assertNotNull(s.createConsumer(q, "  "));
            assertThrows(InvalidDestinationException.class, () -> s.createQueue("has space"));
            assertThrows(InvalidDestinationException.class, () -> s.createQueue(""));
        }
    }

    @Test
    void closedConnectionRejectsEverything() throws Exception {
        Connection c = Jms.factory().createConnection();
        Session s = c.createSession();
        c.close();
        c.close();
        assertThrows(IllegalStateException.class, c::createSession);
        assertThrows(IllegalStateException.class, s::createTextMessage);
    }
}
