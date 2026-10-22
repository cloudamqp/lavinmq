package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.ReturnedMessage;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ReturnIT {
    @Test
    void mandatoryUnroutableIsReturned() throws Exception {
        AmqpConnection conn = Broker.connect();
        AmqpChannel ch = conn.openChannel();
        BlockingQueue<ReturnedMessage> returns = new LinkedBlockingQueue<>();
        ch.setListener(new ChannelListener() {
            @Override
            public void onReturn(ReturnedMessage r) {
                returns.add(r);
            }
        });
        String missing = "no-such-queue-" + System.nanoTime();
        ch.basicPublish("", missing, true, new BasicProperties.Builder().messageId("r1").build(), "payload".getBytes());
        ReturnedMessage r = returns.poll(5, TimeUnit.SECONDS);
        assertNotNull(r);
        assertEquals(312, r.replyCode());
        assertEquals("", r.exchange());
        assertEquals(missing, r.routingKey());
        assertEquals("r1", r.properties().messageId());
        assertEquals("payload", new String(r.body()));
        ch.close();
        conn.close();
    }
}
