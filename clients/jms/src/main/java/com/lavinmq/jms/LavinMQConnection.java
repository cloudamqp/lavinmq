package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.AmqpUri;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ConnectionListener;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.destination.DestinationNames;
import com.lavinmq.jms.destination.LavinMQTemporaryQueue;
import com.lavinmq.jms.destination.LavinMQTemporaryTopic;
import com.lavinmq.jms.message.MessageCodec;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionConsumer;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueSession;
import jakarta.jms.ServerSessionPool;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicSession;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** A JMS connection over one AMQP connection (replaced transparently by automatic recovery). */
public final class LavinMQConnection implements Connection, QueueConnection, TopicConnection,
    LavinMQTemporaryQueue.Owner, LavinMQTemporaryTopic.Owner {
    private static final Logger LOG = System.getLogger(LavinMQConnection.class.getName());
    private static final SecureRandom RANDOM = new SecureRandom();

    final ConnectionSettings settings;
    final String id;
    final MessageCodec codec;
    final ConnectionGate gate = new ConnectionGate();
    final AdminChannel admin = new AdminChannel(this);
    final TopologyCache topology = new TopologyCache();
    final List<LavinMQSession> sessions = new CopyOnWriteArrayList<>();
    final ExecutorService callbacks;
    final RecoveryManager recovery;
    private final AtomicReference<AmqpConnection> transport = new AtomicReference<>();
    private final AtomicInteger generation = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<LavinMQTemporaryQueue> temporaryQueues = new CopyOnWriteArrayList<>();
    private final AtomicLong tempSeq = new AtomicLong();
    private final AtomicInteger addressCursor = new AtomicInteger();
    private volatile ExceptionListener exceptionListener;
    private volatile String clientId;
    private volatile boolean clientIdFrozen;
    private volatile boolean usedForNoLocal;
    private volatile String providerVersion;

    private LavinMQConnection(ConnectionSettings settings) {
        this.settings = settings;
        byte[] rnd = new byte[6];
        RANDOM.nextBytes(rnd);
        this.id = Base64.getUrlEncoder().withoutPadding().encodeToString(rnd);
        this.codec = new MessageCodec(settings.topicExchange, settings.delayedQueueExchange, settings.delayedTopicExchange,
            settings.objectMessageSerialFilter);
        this.clientId = settings.clientId;
        this.callbacks = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lavinmq-jms-callbacks-" + id);
            t.setDaemon(true);
            return t;
        });
        this.recovery = new RecoveryManager(this);
    }

    static LavinMQConnection open(ConnectionSettings settings) throws JMSException {
        LavinMQConnection c = new LavinMQConnection(settings);
        try {
            c.connectTransport(0);
        } catch (IOException e) {
            c.callbacks.shutdown();
            throw JMSExceptions.fromAmqp(e, "connect to " + settings.addresses);
        }
        return c;
    }

    /** Opens the transport to the given address index and installs the shutdown hook. Used initially and by recovery. */
    AmqpConnection connectTransport(int addressIndex) throws IOException {
        AmqpUri.Address address = settings.addresses.get(Math.floorMod(addressIndex, settings.addresses.size()));
        ConnectionConfig cfg = ConnectionConfig.builder()
            .host(address.host()).port(address.port()).tls(settings.tls)
            .username(settings.username).password(settings.password).vhost(settings.virtualHost)
            .sslContext(settings.sslContext).heartbeatSeconds(settings.heartbeatSeconds)
            .connectionTimeoutMs(settings.connectionTimeoutMs).rpcTimeoutMs(settings.rpcTimeoutMs)
            .connectionName("lavinmq-jms-" + id + (settings.connectionNameSuffix == null ? "" : " " + settings.connectionNameSuffix))
            .build();
        AmqpConnection conn = AmqpConnection.open(cfg);
        conn.addListener(new ConnectionListener() {
            @Override
            public void onShutdown(ShutdownSignal signal) {
                transportLost(conn, signal);
            }
        });
        Object v = conn.serverProperties().get("version");
        if (v != null) providerVersion = v.toString();
        transport.set(conn);
        generation.incrementAndGet();
        return conn;
    }

    private void transportLost(AmqpConnection conn, ShutdownSignal signal) {
        if (transport.get() != conn) return; // an old transport from before a recovery
        if (signal.isInitiatedByApplication() || closed.get()) return;
        recovery.onTransportLost(signal);
    }

    // ---- transport access -----------------------------------------------------------------------------------------

    /** The current transport, waiting for a recovery in progress; throws when closed or when recovery takes too long. */
    AmqpConnection usableTransport() throws JMSException {
        checkOpen();
        AmqpConnection t = transport.get();
        if (t != null && t.isOpen() && (!gate.isRecovering() || recovery.isRecoveryThread())) return t;
        try {
            if (!gate.awaitUsable(settings.recoveryTimeoutMs)) {
                checkOpen();
                throw new JMSException("connection is recovering; no broker reachable within " + settings.recoveryTimeoutMs + " ms", "RECOVERING");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JMSException("interrupted while waiting for the connection to recover");
        }
        checkOpen();
        t = transport.get();
        if (t == null || !t.isOpen()) throw new JMSException("connection is not available", "RECOVERING");
        return t;
    }

    AmqpConnection transport() {
        return transport.get();
    }

    int generation() {
        return generation.get();
    }

    int nextAddressIndex() {
        return addressCursor.getAndIncrement();
    }

    void checkOpen() throws IllegalStateException {
        if (closed.get()) {
            Throwable f = gate.failure();
            throw new IllegalStateException("connection is closed" + (f == null ? "" : ": " + f.getMessage()));
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    public String id() {
        return id;
    }

    ConnectionSettings settings() {
        return settings;
    }

    boolean usedForNoLocal() {
        return usedForNoLocal;
    }

    void markNoLocal() {
        usedForNoLocal = true;
    }

    // ---- jakarta.jms.Connection -----------------------------------------------------------------------------------

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return createSession(transacted ? Session.SESSION_TRANSACTED : acknowledgeMode);
    }

    @Override
    public Session createSession(int sessionMode) throws JMSException {
        checkOpen();
        freezeClientId();
        if (sessionMode != Session.AUTO_ACKNOWLEDGE && sessionMode != Session.CLIENT_ACKNOWLEDGE
            && sessionMode != Session.DUPS_OK_ACKNOWLEDGE && sessionMode != Session.SESSION_TRANSACTED) {
            throw new JMSException("invalid session mode " + sessionMode);
        }
        LavinMQSession s = new LavinMQSession(this, sessionMode);
        sessions.add(s);
        try {
            s.openChannel();
        } catch (JMSException e) {
            sessions.remove(s);
            throw e;
        }
        return s;
    }

    @Override
    public Session createSession() throws JMSException {
        return createSession(Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (QueueSession) createSession(transacted, acknowledgeMode);
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (TopicSession) createSession(transacted, acknowledgeMode);
    }

    @Override
    public String getClientID() throws JMSException {
        checkOpen();
        return clientId;
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkOpen();
        if (clientID == null || clientID.isEmpty()) throw new InvalidClientIDException("client id must not be empty");
        if (clientIdFrozen || this.clientId != null) {
            throw new IllegalStateException("client id is already set or the connection has been used");
        }
        this.clientId = clientID;
        clientIdFrozen = true;
    }

    void freezeClientId() {
        clientIdFrozen = true;
    }

    String clientId() {
        return clientId;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkOpen();
        return new LavinMQConnectionMetaData(providerVersion);
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkOpen();
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkOpen();
        freezeClientId();
        this.exceptionListener = listener;
    }

    /** Delivers an exception to the ExceptionListener on the callback thread. */
    void notifyExceptionListener(JMSException e) {
        ExceptionListener l = exceptionListener;
        if (l == null) {
            LOG.log(Level.DEBUG, "no ExceptionListener for: {0}", e.getMessage());
            return;
        }
        try {
            callbacks.execute(() -> {
                try {
                    l.onException(e);
                } catch (RuntimeException ex) {
                    LOG.log(Level.WARNING, "ExceptionListener threw", ex);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // connection already closed
        }
    }

    @Override
    public void start() throws JMSException {
        checkOpen();
        freezeClientId();
        gate.start();
        for (LavinMQSession s : sessions) s.wakeAll();
    }

    @Override
    public void stop() throws JMSException {
        checkOpen();
        freezeClientId();
        for (LavinMQSession s : sessions) s.checkNotOnDispatchThread("Connection.stop()");
        gate.stop();
        for (LavinMQSession s : sessions) {
            s.wakeAll();
            s.awaitListenerIdle();
        }
    }

    @Override
    public void close() throws JMSException {
        if (!closed.compareAndSet(false, true)) return;
        for (LavinMQSession s : sessions) s.checkNotOnDispatchThread("Connection.close()");
        gate.close(null);
        recovery.cancel();
        for (LavinMQSession s : new ArrayList<>(sessions)) {
            try {
                s.closeInternal(true);
            } catch (JMSException e) {
                LOG.log(Level.DEBUG, "closing session failed: {0}", e.getMessage());
            }
        }
        sessions.clear();
        admin.close();
        AmqpConnection t = transport.get();
        if (t != null) {
            try {
                t.close();
            } catch (IOException e) {
                LOG.log(Level.DEBUG, "closing transport failed: {0}", e.getMessage());
            }
        }
        callbacks.shutdown();
        try {
            callbacks.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called by recovery when it gives up: everything is closed and blocked callers fail. */
    void failPermanently(Throwable cause) {
        if (!closed.compareAndSet(false, true)) return;
        gate.close(cause);
        for (LavinMQSession s : new ArrayList<>(sessions)) s.connectionFailed(cause);
        AmqpConnection t = transport.get();
        if (t != null) t.abort();
        notifyExceptionListener(JMSExceptions.fromAmqp(cause, "connection lost"));
        callbacks.shutdown();
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException("connection consumers are not supported");
    }

    // ---- topology -------------------------------------------------------------------------------------------------

    /** Makes sure a JMS queue exists: passive declare first, then (if allowed) a durable declaration. */
    void ensureQueue(String name) throws JMSException {
        if (topology.queues.contains(name)) return;
        DestinationNames.validate(name, "queue");
        if (name.startsWith("amq.") && !DestinationNames.isTempQueueName(name)) {
            throw new InvalidDestinationException("queue names starting with amq. are reserved: " + name);
        }
        try {
            admin.call("declare queue " + name, ch -> ch.queueDeclare(name, true, false, false, false, null));
        } catch (InvalidDestinationException notFound) {
            if (!settings.autoDeclareQueues || DestinationNames.isTempQueueName(name)) throw notFound;
            admin.call("declare queue " + name, ch -> ch.queueDeclare(name, false, true, false, false, null));
        }
        topology.queues.add(name);
    }

    void forgetQueue(String name) {
        topology.queues.remove(name);
    }

    void ensureTopicExchange() throws JMSException {
        if (topology.topicExchange) return;
        String ex = settings.topicExchange;
        if (!ex.startsWith("amq.")) {
            admin.call("declare exchange " + ex, ch -> {
                ch.exchangeDeclare(ex, "topic", false, true, false, false, null);
                return null;
            });
        }
        topology.topicExchange = true;
    }

    void ensureDelayedQueueBinding(String queue) throws JMSException {
        if (!topology.delayedQueueExchange) {
            String ex = settings.delayedQueueExchange;
            admin.call("declare exchange " + ex, ch -> {
                ch.exchangeDeclare(ex, "x-delayed-message", false, true, false, false, Map.of("x-delayed-type", "direct"));
                return null;
            });
            topology.delayedQueueExchange = true;
        }
        if (topology.delayedQueueBindings.contains(queue)) return;
        admin.call("bind queue " + queue + " to " + settings.delayedQueueExchange, ch -> {
            ch.queueBind(queue, settings.delayedQueueExchange, queue, null);
            return null;
        });
        topology.delayedQueueBindings.add(queue);
    }

    void ensureDelayedTopicExchange() throws JMSException {
        if (topology.delayedTopicExchange) return;
        ensureTopicExchange();
        String ex = settings.delayedTopicExchange;
        admin.call("declare exchange " + ex, ch -> {
            ch.exchangeDeclare(ex, "x-delayed-message", false, true, false, false, Map.of("x-delayed-type", "topic"));
            ch.exchangeBind(settings.topicExchange, ex, "#", null);
            return null;
        });
        topology.delayedTopicExchange = true;
    }

    // ---- temporary destinations -----------------------------------------------------------------------------------

    LavinMQTemporaryQueue createTemporaryQueue() throws JMSException {
        checkOpen();
        String name = admin.call("declare temporary queue", ch -> ch.queueDeclare("", false, false, true, false, null).queue());
        LavinMQTemporaryQueue q = new LavinMQTemporaryQueue(name, id, this);
        temporaryQueues.add(q);
        topology.queues.add(name);
        return q;
    }

    LavinMQTemporaryTopic createTemporaryTopic() throws JMSException {
        checkOpen();
        return new LavinMQTemporaryTopic(DestinationNames.tempTopicName(id, tempSeq.incrementAndGet()), id, this);
    }

    List<LavinMQTemporaryQueue> temporaryQueues() {
        return temporaryQueues;
    }

    @Override
    public void deleteTemporaryQueue(LavinMQTemporaryQueue queue) throws JMSException {
        checkOpen();
        for (LavinMQSession s : sessions) {
            if (s.hasConsumerOn(queue)) throw new JMSException("temporary queue " + queue.name() + " still has consumers");
        }
        admin.call("delete temporary queue", ch -> ch.queueDelete(queue.name(), false, false));
        temporaryQueues.remove(queue);
        topology.queues.remove(queue.name());
    }

    @Override
    public void deleteTemporaryTopic(LavinMQTemporaryTopic topic) throws JMSException {
        checkOpen();
        for (LavinMQSession s : sessions) {
            if (s.hasConsumerOn(topic)) throw new JMSException("temporary topic " + topic.name() + " still has consumers");
        }
    }

    /** Opens a channel on the current transport for a single-purpose user (queue browser). */
    AmqpChannel openExtraChannel() throws JMSException {
        try {
            return usableTransport().openChannel();
        } catch (IOException e) {
            throw JMSExceptions.fromAmqp(e, "open channel");
        }
    }

    static boolean isNotFound(Throwable t) {
        return t instanceof ChannelClosedException && ((ChannelClosedException) t).replyCode() == 404;
    }

    @Override
    public String toString() {
        return "LavinMQConnection[" + id + (clientId != null ? ", clientID=" + clientId : "") + "]";
    }
}
