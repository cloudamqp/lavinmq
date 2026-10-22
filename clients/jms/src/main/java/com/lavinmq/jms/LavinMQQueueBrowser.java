package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.GetResponse;
import com.lavinmq.jms.destination.LavinMQQueue;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import java.io.IOException;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Browses a queue with basic.get on a dedicated channel without acknowledging, so the messages stay in the queue;
 * closing the browser closes the channel, which requeues them in order. While a browse is open the browsed messages
 * are not visible to other consumers.
 */
public final class LavinMQQueueBrowser implements QueueBrowser {
    private final LavinMQSession session;
    private final LavinMQQueue queue;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile AmqpChannel channel;

    LavinMQQueueBrowser(LavinMQSession session, LavinMQQueue queue) throws JMSException {
        this.session = session;
        this.queue = queue;
        session.connection.ensureQueue(queue.name());
        this.channel = session.connection.openExtraChannel();
    }

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        checkOpen();
        return null;
    }

    @Override
    public Enumeration<Message> getEnumeration() throws JMSException {
        checkOpen();
        return new Enumeration<>() {
            private Message next;
            private boolean done;

            private void fetch() {
                if (next != null || done) return;
                AmqpChannel ch = channel;
                if (ch == null || !ch.isOpen()) {
                    done = true;
                    if (!closed.get()) throw new JMSRuntimeException("browser channel was closed (connection recovered?)");
                    return;
                }
                try {
                    GetResponse g = ch.basicGet(queue.name(), false);
                    if (g == null) {
                        done = true;
                        return;
                    }
                    next = session.connection.codec.decode(g.properties(), g.body(), g.exchange(), g.routingKey(), g.redelivered());
                } catch (IOException | JMSException e) {
                    done = true;
                    throw new JMSRuntimeException("browse failed: " + e.getMessage(), null, e);
                }
            }

            @Override
            public boolean hasMoreElements() {
                if (closed.get()) return false;
                fetch();
                return next != null;
            }

            @Override
            public Message nextElement() {
                if (closed.get()) throw new NoSuchElementException("browser is closed");
                fetch();
                if (next == null) throw new NoSuchElementException();
                Message m = next;
                next = null;
                return m;
            }
        };
    }

    @Override
    public void close() throws JMSException {
        closeInternal();
        session.browsers.remove(this);
    }

    void closeInternal() {
        if (!closed.compareAndSet(false, true)) return;
        AmqpChannel ch = channel;
        channel = null;
        if (ch != null && ch.isOpen()) {
            try {
                ch.close();
            } catch (IOException ignored) {
                // connection is going away
            }
        }
    }

    /** Recovery: the channel died with the old connection; the enumeration ends with an error. */
    void transportLost() {
        channel = null;
    }

    private void checkOpen() throws JMSException {
        if (closed.get()) throw new IllegalStateException("browser is closed");
        session.checkOpen();
    }
}
