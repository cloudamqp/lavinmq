package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConfirmIT {
    @Test
    void confirmsArriveForEverySequenceNumber() throws Exception {
        AmqpConnection conn = Broker.connect();
        AmqpChannel ch = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        assertEquals(0, ch.basicPublish("", q, false, BasicProperties.EMPTY, "no-confirm".getBytes()));
        ch.confirmSelect();
        assertTrue(ch.confirmMode());
        assertEquals(1, ch.nextPublishSeqNo());
        int n = 100;
        Set<Long> outstanding = new ConcurrentSkipListSet<>();
        CountDownLatch all = new CountDownLatch(1);
        ch.setListener(new ChannelListener() {
            @Override
            public void onConfirm(boolean ack, long deliveryTag, boolean multiple) {
                assertTrue(ack);
                if (multiple) outstanding.removeIf(t -> t <= deliveryTag);
                else outstanding.remove(deliveryTag);
                if (outstanding.isEmpty() && deliveryTag >= n) all.countDown();
            }
        });
        for (int i = 1; i <= n; i++) {
            long seq = ch.basicPublish("", q, false, new BasicProperties.Builder().deliveryMode(2).build(), new byte[10], outstanding::add);
            assertEquals(i, seq);
        }
        assertTrue(all.await(10, TimeUnit.SECONDS), "outstanding: " + outstanding);
        ch.close();
        conn.close();
    }
}
