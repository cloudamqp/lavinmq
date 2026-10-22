package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ChannelListener;
import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.ReturnedMessage;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.destination.DestinationNames;
import com.lavinmq.jms.destination.LavinMQDestination;
import com.lavinmq.jms.destination.LavinMQQueue;
import com.lavinmq.jms.destination.LavinMQTemporaryQueue;
import com.lavinmq.jms.destination.LavinMQTemporaryTopic;
import com.lavinmq.jms.destination.LavinMQTopic;
import com.lavinmq.jms.message.LavinMQBytesMessage;
import com.lavinmq.jms.message.LavinMQMapMessage;
import com.lavinmq.jms.message.LavinMQMessage;
import com.lavinmq.jms.message.LavinMQObjectMessage;
import com.lavinmq.jms.message.LavinMQStreamMessage;
import com.lavinmq.jms.message.LavinMQTextMessage;
import jakarta.jms.BytesMessage;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.JMSException;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.QueueReceiver;
import jakarta.jms.QueueSender;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicPublisher;
import jakarta.jms.TopicSession;
import jakarta.jms.TopicSubscriber;
import jakarta.jms.TransactionRolledBackException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** One JMS session: one AMQP channel, one acknowledgement mode, at most one listener thread. */
public final class LavinMQSession implements Session, QueueSession, TopicSession {
    private static final Logger LOG = System.getLogger(LavinMQSession.class.getName());
    private static final AtomicInteger SEQ = new AtomicInteger();

    record Pick(LavinMQMessageConsumer consumer, Delivery delivery) {
    }

    final LavinMQConnection connection;
    final int number = SEQ.incrementAndGet();
    final int mode;
    final boolean transacted;
    final Object monitor = new Object();
    final AckTracker acks = new AckTracker();
    final ConfirmTracker confirms;
    final List<LavinMQMessageConsumer> consumers = new CopyOnWriteArrayList<>();
    final List<LavinMQMessageProducer> producers = new CopyOnWriteArrayList<>();
    final List<LavinMQQueueBrowser> browsers = new CopyOnWriteArrayList<>();
    private volatile AmqpChannel channel;
    private volatile int channelGeneration;
    private volatile SessionDispatcher dispatcher;
    private volatile MessageListener sessionListener;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile JMSException failure;
    private volatile boolean txWork;
    private volatile boolean txLost;
    private int roundRobin;

    LavinMQSession(LavinMQConnection connection, int mode) {
        this.connection = connection;
        this.mode = mode;
        this.transacted = mode == SESSION_TRANSACTED;
        this.confirms = new ConfirmTracker(connection.callbacks);
    }

    // ---- channel lifecycle ----------------------------------------------------------------------------------------

    void openChannel() throws JMSException {
        AmqpConnection t = connection.usableTransport();
        try {
            AmqpChannel ch = t.openChannel();
            ch.basicQos(connection.settings.prefetch, false);
            if (transacted) ch.txSelect();
            else ch.confirmSelect();
            ch.setListener(new Events(ch));
            channel = ch;
            channelGeneration = connection.generation();
            failure = null;
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "open session channel");
        }
    }

    private final class Events implements ChannelListener {
        private final AmqpChannel ch;

        Events(AmqpChannel ch) {
            this.ch = ch;
        }

        @Override
        public void onReturn(ReturnedMessage r) {
            if (r.exchange().isEmpty()) connection.forgetQueue(r.routingKey());
            confirms.onReturn(r);
        }

        @Override
        public void onConfirm(boolean ack, long deliveryTag, boolean multiple) {
            confirms.onConfirm(ack, deliveryTag, multiple);
        }

        @Override
        public void onShutdown(ShutdownSignal signal) {
            if (ch != channel) return;
            if (signal.isInitiatedByApplication() || signal.isHardError()) return; // close() or connection loss (recovery)
            JMSException e = JMSExceptions.fromAmqp(new ChannelClosedException(signal), "session channel closed by broker");
            channelFailed(e);
        }
    }

    /** The server closed the session channel: the session is unusable until closed. */
    private void channelFailed(JMSException e) {
        failure = e;
        confirms.failAll(e);
        acks.reset();
        wakeAll();
        connection.notifyExceptionListener(e);
    }

    /**
     * A channel-level error happened on this session's channel during {@code context}. If nothing is in flight, a
     * fresh channel replaces the dead one transparently; otherwise the session is marked failed.
     */
    JMSException channelDied(ChannelClosedException cause, String context) {
        JMSException mapped = JMSExceptions.fromAmqp(cause, context);
        boolean clean = consumers.stream().noneMatch(LavinMQMessageConsumer::isActive) && acks.isEmpty() && !txWork && !confirms.hasPending();
        if (clean && !closed.get()) {
            try {
                openChannel();
                return mapped;
            } catch (JMSException e) {
                LOG.log(Level.DEBUG, "could not reopen session channel: {0}", e.getMessage());
            }
        }
        channelFailed(mapped);
        return mapped;
    }

    AmqpChannel channel() throws JMSException {
        checkOpen();
        connection.usableTransport();
        AmqpChannel ch = channel;
        if (ch == null) throw new IllegalStateException("session has no channel");
        return ch;
    }

    int channelGeneration() {
        return channelGeneration;
    }

    boolean isFailed() {
        return failure != null;
    }

    void checkOpen() throws JMSException {
        if (closed.get()) throw new IllegalStateException("session is closed");
        connection.checkOpen();
        JMSException f = failure;
        if (f != null) {
            IllegalStateException e = new IllegalStateException("session is unusable: " + f.getMessage(), f.getErrorCode());
            e.initCause(f);
            throw e;
        }
    }

    // ---- recovery hooks -------------------------------------------------------------------------------------------

    /** The transport is gone: forget everything that lived on the old channel. */
    void transportLost(Throwable cause) {
        JMSException e = JMSExceptions.fromAmqp(cause, "connection lost");
        confirms.failAll(e);
        acks.reset();
        for (LavinMQMessageConsumer c : consumers) c.clearBuffer();
        if (transacted && txWork) txLost = true;
        txWork = false;
        wakeAll();
    }

    /** Re-opens the channel and every consumer on a new transport (recovery thread). */
    void restore() throws JMSException {
        openChannel();
        for (LavinMQMessageConsumer c : consumers) c.resubscribe();
    }

    /** Recovery was given up: fail blocked callers. */
    void connectionFailed(Throwable cause) {
        failure = JMSExceptions.fromAmqp(cause, "connection lost");
        confirms.failAll(failure);
        wakeAll();
        SessionDispatcher d = dispatcher;
        if (d != null && !d.isDispatchThread()) d.stop();
    }

    void wakeAll() {
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }

    // ---- delivery and acknowledgement -----------------------------------------------------------------------------

    void onDelivery(LavinMQMessageConsumer consumer, Delivery d) {
        acks.delivered(d.deliveryTag());
        consumer.enqueue(d);
        wakeAll();
    }

    /** Converts a delivery into a message and applies the acknowledgement rules of the session mode. */
    LavinMQMessage handOver(Delivery d, LavinMQMessageConsumer consumer, boolean synchronous) throws JMSException {
        LavinMQMessage m;
        try {
            m = connection.codec.decode(d.properties(), d.body(), d.exchange(), d.routingKey(), d.redelivered());
        } catch (JMSException | RuntimeException e) {
            try {
                channel.basicNack(d.deliveryTag(), false, false);
            } catch (IOException ignored) {
                // channel is going away
            }
            acks.settled(d.deliveryTag());
            throw e instanceof JMSException ? (JMSException) e : new JMSException("cannot decode message: " + e);
        }
        long tag = d.deliveryTag();
        int gen = channelGeneration;
        switch (mode) {
            case AUTO_ACKNOWLEDGE:
                if (synchronous) {
                    ack(tag, false);
                    acks.settled(tag);
                } else {
                    acks.consumed(tag);
                }
                break;
            case DUPS_OK_ACKNOWLEDGE:
                acks.consumed(tag);
                if (acks.consumedCount() >= connection.settings.dupsOkBatchSize) acknowledgeAll();
                break;
            case CLIENT_ACKNOWLEDGE:
                acks.consumed(tag);
                m.setAckHandle(() -> {
                    checkOpen();
                    if (gen != channelGeneration) {
                        throw new IllegalStateException("message was delivered on a connection that has since been recovered; it will be redelivered");
                    }
                    acknowledgeAll();
                });
                break;
            default: // SESSION_TRANSACTED
                acks.consumed(tag);
                txWork = true;
                break;
        }
        return m;
    }

    /** Runs a listener for one delivery on the dispatch thread. */
    void dispatch(LavinMQMessageConsumer consumer, Delivery d) {
        LavinMQMessage m;
        try {
            m = handOver(d, consumer, false);
        } catch (JMSException e) {
            LOG.log(Level.WARNING, "dropping undecodable message: {0}", e.getMessage());
            return;
        }
        MessageListener l = consumer.listener();
        if (l == null) l = sessionListener;
        boolean ok = false;
        if (l == null) {
            LOG.log(Level.WARNING, "no listener for a dispatched message; requeueing");
        } else {
            try {
                l.onMessage(m);
                ok = true;
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "MessageListener threw; message will be redelivered", e);
            }
        }
        long tag = d.deliveryTag();
        try {
            if (mode == AUTO_ACKNOWLEDGE) {
                if (ok) ack(tag, false);
                else nack(tag, false, true);
                acks.settled(tag);
            } else if (mode == DUPS_OK_ACKNOWLEDGE && !ok) {
                nack(tag, false, true);
                acks.settled(tag);
            }
        } catch (JMSException e) {
            LOG.log(Level.DEBUG, "acknowledging after listener failed: {0}", e.getMessage());
        }
    }

    private void ack(long tag, boolean multiple) throws JMSException {
        try {
            channel.basicAck(tag, multiple);
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "acknowledge");
        }
    }

    private void nack(long tag, boolean multiple, boolean requeue) throws JMSException {
        try {
            channel.basicNack(tag, multiple, requeue);
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "reject");
        }
    }

    /** Acknowledges every message the application has consumed on this session (JMSContext.acknowledge). */
    public void acknowledgeAllConsumed() throws JMSException {
        checkOpen();
        acknowledgeAll();
    }

    /** Acknowledges every message the application has consumed on this session. */
    void acknowledgeAll() throws JMSException {
        for (AckTracker.Op op : acks.drainConsumed()) ack(op.tag(), op.multiple());
    }

    private void requeueConsumed() throws JMSException {
        for (AckTracker.Op op : acks.drainConsumed()) nack(op.tag(), op.multiple(), true);
    }

    /** Requeues deliveries a consumer buffered but never handed to the application. */
    void requeueBuffered(List<Long> tags) {
        for (Long t : acks.drainBuffered(tags)) {
            try {
                channel.basicNack(t, false, true);
            } catch (IOException e) {
                LOG.log(Level.DEBUG, "requeue failed: {0}", e.getMessage());
                return;
            }
        }
    }

    Pick nextListenerDelivery() {
        int n = consumers.size();
        if (n == 0) return null;
        for (int i = 0; i < n; i++) {
            LavinMQMessageConsumer c = consumers.get((roundRobin + i) % n);
            if (c.listener() == null && sessionListener == null) continue;
            Delivery d = c.pollBuffered();
            if (d != null) {
                roundRobin = (roundRobin + i + 1) % n;
                return new Pick(c, d);
            }
        }
        return null;
    }

    // ---- listener management --------------------------------------------------------------------------------------

    void ensureDispatcher() {
        synchronized (monitor) {
            if (dispatcher == null) {
                dispatcher = new SessionDispatcher(this);
                dispatcher.start();
            }
            monitor.notifyAll();
        }
    }

    boolean hasListeners() {
        if (sessionListener != null) return true;
        for (LavinMQMessageConsumer c : consumers) if (c.listener() != null) return true;
        return false;
    }

    void checkNotOnDispatchThread(String what) throws IllegalStateException {
        SessionDispatcher d = dispatcher;
        if (d != null && d.isDispatchThread()) throw new IllegalStateException(what + " must not be called from a MessageListener");
    }

    void awaitListenerIdle() {
        SessionDispatcher d = dispatcher;
        if (d != null) d.awaitIdle();
    }

    boolean hasConsumerOn(LavinMQDestination d) {
        for (LavinMQMessageConsumer c : consumers) if (c.isActive() && c.destination().equals(d)) return true;
        return false;
    }

    // ---- jakarta.jms.Session --------------------------------------------------------------------------------------

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkOpen();
        return new LavinMQBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        checkOpen();
        return new LavinMQMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        checkOpen();
        return new LavinMQMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkOpen();
        LavinMQObjectMessage m = new LavinMQObjectMessage();
        m.setFilterPattern(connection.settings.objectMessageSerialFilter);
        return m;
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        ObjectMessage m = createObjectMessage();
        m.setObject(object);
        return m;
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        checkOpen();
        return new LavinMQStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkOpen();
        return new LavinMQTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkOpen();
        return new LavinMQTextMessage(text);
    }

    @Override
    public boolean getTransacted() throws JMSException {
        checkOpen();
        return transacted;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        checkOpen();
        return mode;
    }

    @Override
    public void commit() throws JMSException {
        checkOpen();
        if (!transacted) throw new IllegalStateException("session is not transacted");
        if (txLost) {
            txLost = false;
            txWork = false;
            throw new TransactionRolledBackException("the transaction was lost when the connection was recovered");
        }
        AmqpChannel ch = channel();
        try {
            acknowledgeAll();
            ch.txCommit();
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "commit");
        }
        txWork = false;
    }

    @Override
    public void rollback() throws JMSException {
        checkOpen();
        if (!transacted) throw new IllegalStateException("session is not transacted");
        txLost = false;
        AmqpChannel ch = channel();
        try {
            ch.txRollback();
            List<AckTracker.Op> ops = acks.drainConsumed();
            for (AckTracker.Op op : ops) ch.basicNack(op.tag(), op.multiple(), true);
            if (!ops.isEmpty()) ch.txCommit(); // the requeues were captured by the transaction; apply them
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "rollback");
        }
        txWork = false;
    }

    @Override
    public void close() throws JMSException {
        closeInternal(false);
    }

    void closeInternal(boolean fromConnection) throws JMSException {
        if (!closed.compareAndSet(false, true)) return;
        SessionDispatcher d = dispatcher;
        if (d != null) d.stop();
        for (LavinMQMessageConsumer c : consumers) c.closeInternal(true);
        consumers.clear();
        for (LavinMQQueueBrowser b : browsers) b.closeInternal();
        browsers.clear();
        producers.clear();
        AmqpChannel ch = channel;
        if (ch != null && ch.isOpen() && failure == null && !connection.gate.isRecovering()) {
            try {
                if (!transacted && confirms.hasPending()) confirms.awaitAll(connection.settings.confirmTimeoutMs);
            } catch (JMSException e) {
                LOG.log(Level.DEBUG, "pending confirms at close: {0}", e.getMessage());
            }
            try {
                ch.close();
            } catch (IOException e) {
                LOG.log(Level.DEBUG, "closing channel failed: {0}", e.getMessage());
            }
        }
        confirms.failAll(new IllegalStateException("session closed"));
        acks.reset();
        if (!fromConnection) connection.sessions.remove(this);
        wakeAll();
    }

    @Override
    public void recover() throws JMSException {
        checkOpen();
        if (transacted) throw new IllegalStateException("recover() is not allowed on a transacted session");
        // Requeue everything the channel holds, buffered or consumed, so the broker redelivers in queue order.
        synchronized (monitor) {
            for (LavinMQMessageConsumer c : consumers) c.clearBuffer();
            long highest = acks.drainAll();
            if (highest >= 0) nack(highest, true, true);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        checkOpen();
        return sessionListener;
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkOpen();
        sessionListener = listener;
        if (listener != null) ensureDispatcher();
    }

    @Override
    public void run() {
        // Application-server session pools are not supported; nothing to do.
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkOpen();
        LavinMQDestination d = destination == null ? null : check(destination);
        LavinMQMessageProducer p = new LavinMQMessageProducer(this, d);
        producers.add(p);
        return p;
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination, null, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException {
        checkOpen();
        checkSelector(messageSelector);
        LavinMQDestination d = check(destination);
        SubscriptionSpec.Kind kind = d.isQueueKind() ? SubscriptionSpec.Kind.QUEUE : SubscriptionSpec.Kind.TOPIC_UNSHARED;
        if (noLocal && d.isQueueKind()) noLocal = false; // noLocal only applies to topics
        return addConsumer(new SubscriptionSpec(kind, d, null, connection.clientId()), noLocal);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) throws JMSException {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) throws JMSException {
        checkOpen();
        checkSelector(messageSelector);
        LavinMQDestination d = checkTopic(topic);
        checkSubscriptionName(sharedSubscriptionName);
        return addConsumer(new SubscriptionSpec(SubscriptionSpec.Kind.TOPIC_SHARED, d, sharedSubscriptionName, connection.clientId()), false);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        return (TopicSubscriber) createDurableConsumer(topic, name, messageSelector, noLocal);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name) throws JMSException {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        checkOpen();
        checkSelector(messageSelector);
        LavinMQDestination d = checkTopic(topic);
        if (d.isTemporary()) throw new InvalidDestinationException("durable subscriptions on temporary topics are not allowed");
        checkSubscriptionName(name);
        if (connection.clientId() == null) throw new IllegalStateException("an unshared durable subscription requires a client id");
        return addConsumer(new SubscriptionSpec(SubscriptionSpec.Kind.DURABLE_UNSHARED, d, name, connection.clientId()), noLocal);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name) throws JMSException {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) throws JMSException {
        checkOpen();
        checkSelector(messageSelector);
        LavinMQDestination d = checkTopic(topic);
        if (d.isTemporary()) throw new InvalidDestinationException("durable subscriptions on temporary topics are not allowed");
        checkSubscriptionName(name);
        return addConsumer(new SubscriptionSpec(SubscriptionSpec.Kind.DURABLE_SHARED, d, name, connection.clientId()), false);
    }

    private MessageConsumer addConsumer(SubscriptionSpec spec, boolean noLocal) throws JMSException {
        connection.freezeClientId();
        if (noLocal) connection.markNoLocal();
        LavinMQMessageConsumer c = new LavinMQMessageConsumer(this, spec, noLocal);
        c.subscribe();
        consumers.add(c);
        wakeAll();
        return c;
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkOpen();
        DestinationNames.validate(queueName, "queue");
        if (DestinationNames.isTempQueueName(queueName)) return new LavinMQTemporaryQueue(queueName);
        return new LavinMQQueue(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        checkOpen();
        DestinationNames.validate(topicName, "topic");
        if (DestinationNames.isTempTopicName(topicName)) return new LavinMQTemporaryTopic(topicName);
        return new LavinMQTopic(topicName);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        checkOpen();
        checkSelector(messageSelector);
        LavinMQDestination d = check(queue);
        if (!d.isQueueKind()) throw new InvalidDestinationException("not a queue: " + queue);
        LavinMQQueueBrowser b = new LavinMQQueueBrowser(this, (LavinMQQueue) d);
        browsers.add(b);
        return b;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        checkOpen();
        connection.freezeClientId();
        return connection.createTemporaryQueue();
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        checkOpen();
        connection.freezeClientId();
        return connection.createTemporaryTopic();
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        checkOpen();
        checkSubscriptionName(name);
        String queue = DestinationNames.durableQueue(connection.clientId(), name);
        for (LavinMQMessageConsumer c : consumers) {
            if (c.isActive() && queue.equals(c.queueName())) {
                throw new IllegalStateException("durable subscription " + name + " has an active consumer in this session");
            }
        }
        try {
            // queue.delete is idempotent on LavinMQ, so check that the subscription exists first (JMS wants an error)
            connection.admin.call("unsubscribe " + name, ch -> ch.queueDeclare(queue, true, false, false, false, null));
        } catch (InvalidDestinationException e) {
            throw new InvalidDestinationException("no durable subscription named '" + name + "'");
        }
        try {
            connection.admin.call("unsubscribe " + name, ch -> ch.queueDelete(queue, true, false));
        } catch (JMSException e) {
            if ("406".equals(e.getErrorCode())) throw new IllegalStateException("durable subscription " + name + " has an active consumer");
            throw e;
        }
    }

    // ---- QueueSession / TopicSession -----------------------------------------------------------------------------

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return (QueueReceiver) createConsumer(queue);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return (QueueReceiver) createConsumer(queue, messageSelector);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) createProducer(queue);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return (TopicSubscriber) createConsumer(topic);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return (TopicSubscriber) createConsumer(topic, messageSelector, noLocal);
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return (TopicPublisher) createProducer(topic);
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    static LavinMQDestination check(Destination d) throws JMSException {
        if (d == null) throw new InvalidDestinationException("destination is null");
        if (!(d instanceof LavinMQDestination)) throw new InvalidDestinationException("not a LavinMQ destination: " + d);
        return (LavinMQDestination) d;
    }

    private static LavinMQDestination checkTopic(Topic t) throws JMSException {
        LavinMQDestination d = check(t);
        if (!d.isTopicKind()) throw new InvalidDestinationException("not a topic: " + t);
        return d;
    }

    static void checkSelector(String selector) throws InvalidSelectorException {
        if (selector != null && !selector.isBlank()) {
            throw new InvalidSelectorException("message selectors are not supported by LavinMQ");
        }
    }

    private static void checkSubscriptionName(String name) throws JMSException {
        if (name == null || name.isEmpty()) throw new JMSException("subscription name must not be empty");
    }

    boolean isTransactedWithWork() {
        return transacted && txWork;
    }

    void markTxWork() {
        txWork = true;
    }

    @Override
    public String toString() {
        return "LavinMQSession[" + number + ", mode=" + mode + "]";
    }
}
