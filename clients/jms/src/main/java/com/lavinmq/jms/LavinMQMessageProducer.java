package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.destination.LavinMQDestination;
import com.lavinmq.jms.message.LavinMQMessage;
import com.lavinmq.jms.message.MessageCodec;
import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Publishes messages on the session channel; waits for publisher confirms as configured. */
public final class LavinMQMessageProducer implements MessageProducer, QueueSender, TopicPublisher {
    private static final AtomicLong MESSAGE_SEQ = new AtomicLong();

    private final LavinMQSession session;
    private final LavinMQDestination destination;
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive;
    private long deliveryDelay;

    LavinMQMessageProducer(LavinMQSession session, LavinMQDestination destination) {
        this.session = session;
        this.destination = destination;
    }

    private void checkOpen() throws JMSException {
        if (closed.get()) throw new IllegalStateException("producer is closed");
        session.checkOpen();
    }

    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        checkOpen();
        disableMessageID = value;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        checkOpen();
        return disableMessageID;
    }

    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        checkOpen();
        disableMessageTimestamp = value;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        checkOpen();
        return disableMessageTimestamp;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        checkOpen();
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSException("invalid delivery mode " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        checkOpen();
        return deliveryMode;
    }

    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        checkOpen();
        if (defaultPriority < 0 || defaultPriority > 9) throw new JMSException("priority out of range: " + defaultPriority);
        this.priority = defaultPriority;
    }

    @Override
    public int getPriority() throws JMSException {
        checkOpen();
        return priority;
    }

    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        checkOpen();
        if (timeToLive < 0) throw new JMSException("time to live must not be negative");
        this.timeToLive = timeToLive;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        checkOpen();
        return timeToLive;
    }

    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        checkOpen();
        if (deliveryDelay < 0) throw new JMSException("delivery delay must not be negative");
        this.deliveryDelay = deliveryDelay;
    }

    @Override
    public long getDeliveryDelay() throws JMSException {
        checkOpen();
        return deliveryDelay;
    }

    @Override
    public Destination getDestination() throws JMSException {
        checkOpen();
        return destination;
    }

    @Override
    public void close() throws JMSException {
        if (!closed.compareAndSet(false, true)) return;
        session.producers.remove(this);
        if (!session.transacted && !session.isFailed()) {
            try {
                session.confirms.awaitAll(session.connection.settings.confirmTimeoutMs);
            } catch (JMSException ignored) {
                // outstanding sends already reported their outcome
            }
        }
    }

    // ---- send -----------------------------------------------------------------------------------------------------

    @Override
    public void send(Message message) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (destination == null) throw new UnsupportedOperationException("this producer was created without a destination");
        doSend(destination, message, deliveryMode, priority, timeToLive, null);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (this.destination != null) throw new UnsupportedOperationException("this producer was created with a destination");
        doSend(LavinMQSession.check(destination), message, deliveryMode, priority, timeToLive, null);
    }

    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
        send(message, deliveryMode, priority, timeToLive, completionListener);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        if (destination == null) throw new UnsupportedOperationException("this producer was created without a destination");
        if (completionListener == null) throw new IllegalArgumentException("completionListener is null");
        doSend(destination, message, deliveryMode, priority, timeToLive, completionListener);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive, completionListener);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        if (this.destination != null) throw new UnsupportedOperationException("this producer was created with a destination");
        if (completionListener == null) throw new IllegalArgumentException("completionListener is null");
        doSend(LavinMQSession.check(destination), message, deliveryMode, priority, timeToLive, completionListener);
    }

    void doSend(LavinMQDestination dest, Message message, int dm, int prio, long ttl, CompletionListener listener) throws JMSException {
        checkOpen();
        if (message == null) throw new MessageFormatException("message is null");
        if (dm != DeliveryMode.PERSISTENT && dm != DeliveryMode.NON_PERSISTENT) throw new JMSException("invalid delivery mode " + dm);
        if (prio < 0 || prio > 9) throw new JMSException("priority out of range: " + prio);
        if (ttl < 0) throw new JMSException("time to live must not be negative");
        LavinMQConnection conn = session.connection;
        LavinMQMessage m = MessageCodec.toLavinMQ(message);
        long now = System.currentTimeMillis();
        long delay = deliveryDelay;
        m.setJMSDestination(dest);
        m.setJMSDeliveryMode(dm);
        m.setJMSPriority(prio);
        m.setJMSTimestamp(disableMessageTimestamp ? 0 : now);
        m.setJMSExpiration(ttl > 0 ? now + ttl : 0);
        m.setJMSDeliveryTime(now + delay);
        m.setJMSMessageID(disableMessageID ? null : "ID:" + conn.id + "-" + MESSAGE_SEQ.incrementAndGet());
        m.setJMSRedelivered(false);
        if (conn.usedForNoLocal()) m.setPropertyInternal(MessageCodec.JMSX_CONNECTION_ID, conn.id);

        String exchange;
        String routingKey = dest.name();
        boolean mandatory;
        if (dest.isQueueKind()) {
            if (!dest.isTemporary()) conn.ensureQueue(dest.name());
            mandatory = true;
            if (delay > 0) {
                conn.ensureDelayedQueueBinding(dest.name());
                exchange = conn.settings.delayedQueueExchange;
            } else {
                exchange = "";
            }
        } else {
            mandatory = false;
            if (dest.exchange() != null) {
                exchange = dest.exchange();
            } else if (delay > 0) {
                conn.ensureDelayedTopicExchange();
                exchange = conn.settings.delayedTopicExchange;
            } else {
                conn.ensureTopicExchange();
                exchange = conn.settings.topicExchange;
            }
        }
        if (delay > 0xFFFFFFFFL) throw new JMSException("delivery delay larger than 2^32-1 ms is not supported");
        MessageCodec.Encoded enc = conn.codec.encode(m, disableMessageID, disableMessageTimestamp, ttl, delay);

        AmqpChannel ch = session.channel();
        try {
            if (session.transacted) {
                ch.basicPublish(exchange, routingKey, mandatory, enc.properties(), enc.body());
                session.markTxWork();
                if (listener != null) conn.callbacks.execute(() -> listener.onCompletion(m));
                return;
            }
            ConfirmTracker.Pending[] holder = new ConfirmTracker.Pending[1];
            ch.basicPublish(exchange, routingKey, mandatory, enc.properties(), enc.body(),
                seq -> holder[0] = session.confirms.register(seq, listener, m, enc.properties().messageId()));
            if (listener == null && shouldWait(dm) && holder[0] != null) {
                try {
                    holder[0].future.get(conn.settings.confirmTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof JMSException) throw (JMSException) e.getCause();
                    throw JMSExceptions.wrap("send failed", e.getCause());
                } catch (TimeoutException e) {
                    throw new JMSException("no publisher confirm within " + conn.settings.confirmTimeoutMs + " ms", "TIMEOUT");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new JMSException("interrupted while waiting for the publisher confirm");
                }
            }
        } catch (ChannelClosedException e) {
            throw session.channelDied(e, "send to " + dest);
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "send to " + dest);
        }
    }

    private boolean shouldWait(int dm) {
        switch (session.connection.settings.publisherConfirms) {
            case ALWAYS: return true;
            case NEVER: return false;
            default: return dm == DeliveryMode.PERSISTENT;
        }
    }

    // ---- QueueSender / TopicPublisher -----------------------------------------------------------------------------

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return destination != null && destination.isQueueKind() ? (Queue) destination : null;
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        send((Destination) queue, message);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send((Destination) queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public Topic getTopic() throws JMSException {
        checkOpen();
        return destination != null && destination.isTopicKind() ? (Topic) destination : null;
    }

    @Override
    public void publish(Message message) throws JMSException {
        send(message);
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        send((Destination) topic, message);
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send((Destination) topic, message, deliveryMode, priority, timeToLive);
    }

}
