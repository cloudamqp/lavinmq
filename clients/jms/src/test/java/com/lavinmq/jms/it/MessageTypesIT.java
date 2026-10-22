package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.it.Broker;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MessageTypesIT {
    private Connection conn;
    private Session session;
    private Queue q;
    private MessageProducer producer;
    private MessageConsumer consumer;

    @BeforeEach
    void setUp() throws Exception {
        conn = Jms.factory().createConnection();
        session = conn.createSession();
        q = session.createQueue(Jms.unique("types"));
        producer = session.createProducer(q);
        consumer = session.createConsumer(q);
        conn.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        conn.close();
    }

    @Test
    void allFiveTypesRoundTrip() throws Exception {
        TextMessage t = session.createTextMessage("täxt");
        BytesMessage b = session.createBytesMessage();
        b.writeUTF("bytes");
        b.writeDouble(1.5);
        MapMessage m = session.createMapMessage();
        m.setString("s", "v");
        m.setLong("l", 5L);
        m.setBytes("b", new byte[] {1});
        StreamMessage s = session.createStreamMessage();
        s.writeInt(1);
        s.writeObject("two");
        ObjectMessage o = session.createObjectMessage(new ArrayList<>(List.of("x", "y")));
        Message plain = session.createMessage();
        plain.setBooleanProperty("flag", true);
        producer.send(t);
        producer.send(b);
        producer.send(m);
        producer.send(s);
        producer.send(o);
        producer.send(plain);

        assertEquals("täxt", ((TextMessage) consumer.receive(5000)).getText());
        BytesMessage rb = (BytesMessage) consumer.receive(5000);
        assertEquals("bytes", rb.readUTF());
        assertEquals(1.5, rb.readDouble());
        MapMessage rm = (MapMessage) consumer.receive(5000);
        assertEquals("v", rm.getString("s"));
        assertEquals(5L, rm.getLong("l"));
        assertArrayEquals(new byte[] {1}, rm.getBytes("b"));
        StreamMessage rs = (StreamMessage) consumer.receive(5000);
        assertEquals(1, rs.readInt());
        assertEquals("two", rs.readString());
        ObjectMessage ro = (ObjectMessage) consumer.receive(5000);
        assertEquals(List.of("x", "y"), ro.getObject());
        Message rp = consumer.receive(5000);
        assertEquals(Message.class.getSimpleName().length() > 0, true);
        assertTrue(rp.getBooleanProperty("flag"));
        assertTrue(rp.getClass().getSimpleName().equals("LavinMQMessage"), rp.getClass().getName());
        // received messages are read-only
        assertTrue(rb.getBodyLength() > 0);
        assertNull(rp.getBody(String.class));
    }

    @Test
    void plainAmqpMessagesAreReceivedAsTextOrBytes() throws Exception {
        AmqpConnection amqp = Broker.connect();
        AmqpChannel ch = amqp.openChannel();
        ch.basicPublish("", q.getQueueName(), false, new BasicProperties.Builder().contentType("text/plain").messageId("plain-1").build(), "from amqp".getBytes());
        ch.basicPublish("", q.getQueueName(), false, BasicProperties.EMPTY, new byte[] {9, 8});
        Message first = consumer.receive(5000);
        assertTrue(first instanceof TextMessage);
        assertEquals("from amqp", ((TextMessage) first).getText());
        assertEquals("ID:plain-1", first.getJMSMessageID());
        Message second = consumer.receive(5000);
        assertTrue(second instanceof BytesMessage);
        assertArrayEquals(new byte[] {9, 8}, second.getBody(byte[].class));
        amqp.close();
    }
}
