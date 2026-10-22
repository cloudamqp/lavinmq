package com.lavinmq.jms.amqp.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.AuthenticationFailureException;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.ConnectionListener;
import com.lavinmq.jms.amqp.ShutdownSignal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ConnectionIT {
    @Test
    void openAndClose() throws Exception {
        AmqpConnection c = Broker.connect();
        assertTrue(c.isOpen());
        assertEquals("LavinMQ", c.serverProperties().get("product"));
        assertTrue(c.frameMax() >= 4096);
        assertTrue(c.channelMax() > 0);
        assertEquals(10, c.heartbeatSeconds());
        CountDownLatch shutdown = new CountDownLatch(1);
        AtomicReference<ShutdownSignal> sig = new AtomicReference<>();
        c.addListener(new ConnectionListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                sig.set(signal);
                shutdown.countDown();
            }
        });
        c.close();
        assertFalse(c.isOpen());
        assertTrue(shutdown.await(5, TimeUnit.SECONDS));
        assertTrue(sig.get().isInitiatedByApplication());
        assertEquals(200, sig.get().replyCode());
        assertThrows(ConnectionClosedException.class, c::openChannel);
    }

    @Test
    void badPassword() {
        AuthenticationFailureException e = assertThrows(AuthenticationFailureException.class,
            () -> AmqpConnection.open(Broker.config().password("definitely-wrong").build()));
        assertEquals(403, e.replyCode());
        assertTrue(e.getMessage().contains("ACCESS_REFUSED"), e.getMessage());
    }

    @Test
    void unknownVhost() {
        ConnectionClosedException e = assertThrows(ConnectionClosedException.class,
            () -> AmqpConnection.open(Broker.config().vhost("no-such-vhost-" + System.nanoTime()).build()));
        assertEquals(530, e.replyCode());
    }

    @Test
    void heartbeatsKeepConnectionAlive() throws Exception {
        AmqpConnection c = AmqpConnection.open(Broker.config().heartbeatSeconds(1).build());
        assertEquals(1, c.heartbeatSeconds());
        Thread.sleep(4000);
        assertTrue(c.isOpen());
        assertNotNull(c.openChannel());
        c.close();
    }

    @Test
    void abortIsImmediate() throws Exception {
        AmqpConnection c = Broker.connect();
        c.abort();
        assertFalse(c.isOpen());
    }
}
