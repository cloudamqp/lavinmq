package com.lavinmq.jms.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AmqpUriTest {
    @Test
    void defaults() {
        AmqpUri u = AmqpUri.parse("amqp://localhost");
        assertFalse(u.tls());
        assertEquals("localhost", u.host());
        assertEquals(5672, u.port());
        assertEquals("guest", u.username());
        assertEquals("guest", u.password());
        assertEquals("/", u.vhost());
        assertNull(u.heartbeatSeconds());
        assertEquals("/", AmqpUri.parse("amqp://h/").vhost());
        assertEquals("localhost", AmqpUri.parse("amqp://").host());
    }

    @Test
    void tlsDefaultsPort() {
        AmqpUri u = AmqpUri.parse("amqps://broker.example.com");
        assertTrue(u.tls());
        assertEquals(5671, u.port());
        assertEquals(5673, AmqpUri.parse("amqps://broker.example.com:5673").port());
    }

    @Test
    void percentDecoding() {
        AmqpUri u = AmqpUri.parse("amqp://u%40x:p%3Aw@h:5673/%2Fvh");
        assertEquals("u@x", u.username());
        assertEquals("p:w", u.password());
        assertEquals("h", u.host());
        assertEquals(5673, u.port());
        assertEquals("/vh", u.vhost());
        assertEquals("/", AmqpUri.parse("amqp://h/%2F").vhost());
        assertEquals("prod", AmqpUri.parse("amqp://h/prod").vhost());
        assertEquals("a+b", AmqpUri.parse("amqp://u:a+b@h").password());
    }

    @Test
    void ipv6AndMultipleHosts() {
        AmqpUri u = AmqpUri.parse("amqp://[::1]:5672");
        assertEquals("::1", u.host());
        assertEquals(5672, u.port());
        AmqpUri m = AmqpUri.parse("amqp://u:p@h1:5672,h2,[::1]:5680/vh");
        assertEquals(3, m.addresses().size());
        assertEquals(new AmqpUri.Address("h1", 5672), m.addresses().get(0));
        assertEquals(new AmqpUri.Address("h2", 5672), m.addresses().get(1));
        assertEquals(new AmqpUri.Address("::1", 5680), m.addresses().get(2));
        assertEquals("vh", m.vhost());
        assertEquals("[::1]:5680", m.addresses().get(2).toString());
    }

    @Test
    void queryParameters() {
        AmqpUri u = AmqpUri.parse("amqp://h/?heartbeat=10&connection_timeout=5000&other=x");
        assertEquals(10, u.heartbeatSeconds());
        assertEquals(5000, u.connectionTimeoutMs());
        assertEquals("x", u.query().get("other"));
        assertThrows(IllegalArgumentException.class, () -> AmqpUri.parse("amqp://h?heartbeat=abc").heartbeatSeconds());
    }

    @Test
    void badInput() {
        assertThrows(IllegalArgumentException.class, () -> AmqpUri.parse("http://h"));
        assertThrows(IllegalArgumentException.class, () -> AmqpUri.parse("localhost"));
        assertThrows(IllegalArgumentException.class, () -> AmqpUri.parse(""));
        assertThrows(IllegalArgumentException.class, () -> AmqpUri.parse("amqp://h1,,h2"));
    }
}
