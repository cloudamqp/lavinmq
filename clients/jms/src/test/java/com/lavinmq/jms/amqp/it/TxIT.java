package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.GetResponse;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import org.junit.jupiter.api.Test;

class TxIT {
    @Test
    void commitAndRollback() throws Exception {
        AmqpConnection conn = Broker.connect();
        AmqpChannel ch = conn.openChannel();
        AmqpChannel reader = conn.openChannel();
        String q = ch.queueDeclare("", false, false, true, true, null).queue();
        ch.txSelect();
        ch.basicPublish("", q, false, BasicProperties.EMPTY, "rolled-back".getBytes());
        ch.txRollback();
        Thread.sleep(100);
        assertNull(reader.basicGet(q, true));
        ch.basicPublish("", q, false, BasicProperties.EMPTY, "committed".getBytes());
        ch.txCommit();
        GetResponse g = reader.basicGet(q, true);
        assertNotNull(g);
        assertEquals("committed", new String(g.body()));
        // confirm.select on a tx channel is a precondition failure
        ChannelClosedException e = assertThrows(ChannelClosedException.class, ch::confirmSelect);
        assertEquals(406, e.replyCode());
        reader.close();
        conn.close();
    }
}
