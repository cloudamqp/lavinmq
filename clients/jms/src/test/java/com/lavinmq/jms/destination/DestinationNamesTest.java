package com.lavinmq.jms.destination;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.InvalidDestinationException;
import org.junit.jupiter.api.Test;

class DestinationNamesTest {
    @Test
    void validation() throws Exception {
        assertEquals("orders.new", DestinationNames.validate("orders.new", "queue"));
        assertThrows(InvalidDestinationException.class, () -> DestinationNames.validate("", "queue"));
        assertThrows(InvalidDestinationException.class, () -> DestinationNames.validate(null, "queue"));
        assertThrows(InvalidDestinationException.class, () -> DestinationNames.validate("with space", "queue"));
        assertThrows(InvalidDestinationException.class, () -> DestinationNames.validate("ünïcode", "queue"));
        assertThrows(InvalidDestinationException.class, () -> DestinationNames.validate("a".repeat(256), "queue"));
    }

    @Test
    void subscriptionQueueNames() {
        assertEquals("jms.durable.app-1.sub", DestinationNames.durableQueue("app-1", "sub"));
        assertEquals("jms.durable..sub", DestinationNames.durableQueue(null, "sub"));
        assertEquals("jms.shared.c.my%2Esub%20x", DestinationNames.sharedQueue("c", "my.sub x"));
        assertEquals("jms.durable.c.100%25", DestinationNames.durableQueue("c", "100%"));
        assertEquals("%C3%A9", DestinationNames.encode("é"));
    }

    @Test
    void temporaryNames() {
        String t = DestinationNames.tempTopicName("conn1", 7);
        assertEquals("jms.tmp.conn1.7", t);
        assertTrue(DestinationNames.isTempTopicName(t));
        assertEquals("conn1", DestinationNames.tempTopicOwner(t));
        assertEquals("conn1", new LavinMQTemporaryTopic(t).ownerConnectionId());
        assertTrue(DestinationNames.isTempQueueName("amq.gen-abc"));
        assertFalse(DestinationNames.isTempQueueName("orders"));
    }

    @Test
    void replyToRoundTrip() {
        LavinMQDestination[] ds = {
            new LavinMQQueue("q"), new LavinMQTopic("t.x"), new LavinMQTemporaryQueue("amq.gen-1", "c", null),
            new LavinMQTemporaryTopic("jms.tmp.c.1", "c", null)};
        String[] types = {"queue", "topic", "temp-queue", "temp-topic"};
        for (int i = 0; i < ds.length; i++) {
            assertEquals(types[i], DestinationNames.replyToType(ds[i]));
            LavinMQDestination back = DestinationNames.fromReplyTo(ds[i].name(), types[i]);
            assertEquals(ds[i].kind(), back.kind());
            assertEquals(ds[i].name(), back.name());
            assertEquals(ds[i], back);
        }
        // plain AMQP reply-to without type header: queue, unless the name prefix says otherwise
        assertEquals(LavinMQDestination.Kind.QUEUE, DestinationNames.fromReplyTo("rpc", null).kind());
        assertEquals(LavinMQDestination.Kind.TEMP_QUEUE, DestinationNames.fromReplyTo("amq.gen-x", null).kind());
        assertEquals(LavinMQDestination.Kind.TEMP_TOPIC, DestinationNames.fromReplyTo("jms.tmp.c.2", null).kind());
    }

    @Test
    void destinationFromDelivery() {
        LavinMQDestination q = DestinationNames.fromDelivery("", "orders", "jms.topic", "jms.delayed", "jms.delayed.topic");
        assertEquals(new LavinMQQueue("orders"), q);
        assertTrue(DestinationNames.fromDelivery("jms.delayed", "orders", "jms.topic", "jms.delayed", "jms.delayed.topic") instanceof LavinMQQueue);
        LavinMQDestination t = DestinationNames.fromDelivery("jms.topic", "news.eu", "jms.topic", "jms.delayed", "jms.delayed.topic");
        assertEquals(new LavinMQTopic("news.eu"), t);
        assertTrue(DestinationNames.fromDelivery("jms.delayed.topic", "news", "jms.topic", "jms.delayed", "jms.delayed.topic") instanceof LavinMQTopic);
        LavinMQDestination foreign = DestinationNames.fromDelivery("amq.topic", "k", "jms.topic", "jms.delayed", "jms.delayed.topic");
        assertEquals("amq.topic", foreign.exchange());
        assertTrue(foreign instanceof LavinMQTopic);
        assertTrue(DestinationNames.fromDelivery("", "amq.gen-1", "jms.topic", "jms.delayed", "jms.delayed.topic").isTemporary());
        assertEquals("queue://orders", q.toString());
    }
}
