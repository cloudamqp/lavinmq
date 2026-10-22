package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ConsumerCallback;
import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.destination.LavinMQDestination;
import com.lavinmq.jms.destination.LavinMQTemporaryQueue;
import com.lavinmq.jms.destination.LavinMQTemporaryTopic;
import com.lavinmq.jms.message.MessageCodec;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Queue;
import jakarta.jms.QueueReceiver;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** A consumer on one AMQP queue, with a local buffer for synchronous receives and listener dispatch. */
public final class LavinMQMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber {
    private final LavinMQSession session;
    private final SubscriptionSpec spec;
    private final boolean noLocal;
    private final LinkedBlockingDeque<Delivery> buffer = new LinkedBlockingDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile String queueName;
    private volatile String consumerTag;
    private volatile MessageListener listener;
    private volatile boolean cancelledByServer;
    private volatile int receiving;

    LavinMQMessageConsumer(LavinMQSession session, SubscriptionSpec spec, boolean noLocal) {
        this.session = session;
        this.spec = spec;
        this.noLocal = noLocal;
    }

    LavinMQDestination destination() {
        return spec.destination;
    }

    String queueName() {
        return queueName;
    }

    MessageListener listener() {
        return listener;
    }

    boolean isActive() {
        return !closed.get() && !cancelledByServer;
    }

    // ---- subscription ---------------------------------------------------------------------------------------------

    /** Resolves the queue (declaring and binding as needed) and starts the AMQP consumer. */
    void subscribe() throws JMSException {
        LavinMQConnection conn = session.connection;
        LavinMQDestination d = spec.destination;
        String queue;
        switch (spec.kind) {
            case QUEUE:
                if (d instanceof LavinMQTemporaryQueue && !((LavinMQTemporaryQueue) d).isOwnedBy(conn.id)) {
                    throw new InvalidDestinationException("temporary queue " + d.name() + " belongs to another connection");
                }
                conn.ensureQueue(d.name());
                queue = d.name();
                break;
            case TOPIC_UNSHARED:
                if (d instanceof LavinMQTemporaryTopic && !((LavinMQTemporaryTopic) d).isOwnedBy(conn.id)) {
                    throw new InvalidDestinationException("temporary topic " + d.name() + " belongs to another connection");
                }
                conn.ensureTopicExchange();
                queue = conn.admin.call("declare subscription queue", ch -> ch.queueDeclare("", false, false, true, true, null).queue());
                bind(queue);
                break;
            default:
                conn.ensureTopicExchange();
                queue = spec.queueName();
                declareSubscriptionQueue(queue);
                bind(queue);
                break;
        }
        queueName = queue;
        AmqpChannel ch = session.channel();
        try {
            consumerTag = ch.basicConsume(queue, "", false, spec.exclusiveConsume(), null, new Callback());
        } catch (ChannelClosedException e) {
            JMSException mapped = session.channelDied(e, "consume from " + queue);
            if (e.replyCode() == 403 && spec.exclusiveConsume()) {
                JMSException j = new JMSException("durable subscription '" + spec.subscriptionName + "' already has an active consumer", "403");
                j.setLinkedException(e);
                throw j;
            }
            throw mapped;
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "consume from " + queue);
        }
    }

    /** After a recovery: the old consumer tag is gone, server-named queues must be re-created. */
    void resubscribe() throws JMSException {
        if (!isActive()) return;
        buffer.clear();
        subscribe();
    }

    private void bind(String queue) throws JMSException {
        String ex = session.connection.settings.topicExchange;
        session.connection.admin.call("bind subscription queue", ch -> {
            ch.queueBind(queue, ex, spec.destination.name(), null);
            return null;
        });
    }

    private void declareSubscriptionQueue(String queue) throws JMSException {
        boolean durable = spec.isDurable();
        Map<String, Object> args = Map.of("x-jms-topic", spec.destination.name(), "x-jms-shared", spec.isShared());
        LavinMQConnection conn = session.connection;
        try {
            conn.admin.call("declare subscription queue " + queue, ch -> ch.queueDeclare(queue, false, durable, false, !durable, args));
        } catch (JMSException e) {
            if (!"406".equals(e.getErrorCode())) throw e;
            // the subscription exists for another topic (or with another sharing mode): JMS says delete and recreate
            try {
                conn.admin.call("delete subscription queue " + queue, ch -> ch.queueDelete(queue, true, false));
            } catch (JMSException del) {
                if ("406".equals(del.getErrorCode())) {
                    throw new JMSException("subscription '" + spec.subscriptionName + "' exists for a different topic and has an active consumer", "406");
                }
                throw del;
            }
            conn.admin.call("declare subscription queue " + queue, ch -> ch.queueDeclare(queue, false, durable, false, !durable, args));
        }
    }

    private final class Callback implements ConsumerCallback {
        @Override
        public void handleDelivery(Delivery d) {
            if (closed.get()) {
                requeue(d.deliveryTag());
                return;
            }
            if (noLocal && isLocal(d)) {
                try {
                    session.channel().basicAck(d.deliveryTag(), false);
                } catch (JMSException | IOException ignored) {
                    // channel is going away
                }
                return;
            }
            session.onDelivery(LavinMQMessageConsumer.this, d);
        }

        @Override
        public void handleCancel(String tag) {
            cancelledByServer = true;
            session.wakeAll();
        }

        @Override
        public void handleShutdown(String tag, ShutdownSignal signal) {
            // the session handles channel and connection loss
        }
    }

    private boolean isLocal(Delivery d) {
        Map<String, Object> h = d.properties().headers();
        return h != null && session.connection.id.equals(h.get(MessageCodec.JMSX_CONNECTION_ID));
    }

    private void requeue(long tag) {
        try {
            session.channel().basicNack(tag, false, true);
        } catch (JMSException | IOException ignored) {
            // channel is going away
        }
    }

    void enqueue(Delivery d) {
        buffer.add(d);
    }

    Delivery pollBuffered() {
        return buffer.pollFirst();
    }

    void clearBuffer() {
        buffer.clear();
    }

    // ---- jakarta.jms.MessageConsumer ------------------------------------------------------------------------------

    @Override
    public String getMessageSelector() throws JMSException {
        checkOpen();
        return null;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkOpen();
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkOpen();
        if (receiving > 0) throw new IllegalStateException("a synchronous receive is in progress");
        this.listener = listener;
        if (listener != null) session.ensureDispatcher();
    }

    @Override
    public Message receive() throws JMSException {
        return receive(0);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        return doReceive(timeout, false);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return doReceive(0, true);
    }

    private Message doReceive(long timeoutMs, boolean noWait) throws JMSException {
        checkOpen();
        if (session.hasListeners()) throw new IllegalStateException("the session is being used asynchronously with a MessageListener");
        session.checkNotOnDispatchThread("receive()");
        long deadline = timeoutMs > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs) : 0;
        Delivery d;
        receiving++;
        try {
            synchronized (session.monitor) {
                while (true) {
                    if (closed.get() || cancelledByServer) return null;
                    session.checkOpen();
                    if (session.connection.gate.isStarted()) {
                        d = buffer.pollFirst();
                        if (d != null) break;
                    }
                    if (noWait) return null;
                    long waitMs = 0;
                    if (deadline != 0) {
                        long remaining = deadline - System.nanoTime();
                        if (remaining <= 0) return null;
                        waitMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining));
                    }
                    try {
                        session.monitor.wait(waitMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new JMSException("interrupted while waiting for a message");
                    }
                }
            }
        } finally {
            receiving--;
        }
        return session.handOver(d, this, true);
    }

    @Override
    public void close() throws JMSException {
        closeInternal(false);
    }

    void closeInternal(boolean fromSession) throws JMSException {
        if (!closed.compareAndSet(false, true)) return;
        if (!fromSession) session.checkNotOnDispatchThread("MessageConsumer.close()");
        String tag = consumerTag;
        AmqpChannel ch = null;
        try {
            if (!session.connection.gate.isRecovering() && !session.connection.isClosed()) ch = session.channel();
        } catch (JMSException ignored) {
            // session or connection already closed
        }
        if (ch != null && ch.isOpen() && tag != null && !cancelledByServer) {
            try {
                ch.basicCancel(tag);
            } catch (IOException ignored) {
                // channel is going away
            }
        }
        List<Long> tags = new ArrayList<>();
        for (Delivery d : buffer) tags.add(d.deliveryTag());
        buffer.clear();
        if (ch != null && ch.isOpen() && !tags.isEmpty()) session.requeueBuffered(tags);
        if (!fromSession) session.consumers.remove(this);
        session.wakeAll();
    }

    private void checkOpen() throws JMSException {
        if (closed.get()) throw new IllegalStateException("consumer is closed");
        session.checkOpen();
    }

    // ---- QueueReceiver / TopicSubscriber --------------------------------------------------------------------------

    @Override
    public Queue getQueue() throws JMSException {
        checkOpen();
        return spec.destination.isQueueKind() ? (Queue) spec.destination : null;
    }

    @Override
    public Topic getTopic() throws JMSException {
        checkOpen();
        return spec.destination.isTopicKind() ? (Topic) spec.destination : null;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        checkOpen();
        return noLocal;
    }

    @Override
    public String toString() {
        return "LavinMQMessageConsumer[" + spec.destination + " via " + queueName + "]";
    }
}
