package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpUri;
import com.lavinmq.jms.context.LavinMQJMSContext;
import com.lavinmq.jms.message.ObjectMessageDeserializer;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.net.ssl.SSLContext;

/**
 * Entry point of the client. Configure with bean setters or an {@code amqp://} URL, then create connections or contexts.
 *
 * <p>URL query parameters override setters called before {@link #setUrl(String)}; setters called later win. Several
 * hosts may be listed for fail-over: {@code amqp://user:pass@host1:5672,host2:5672/vhost}.
 */
public class LavinMQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory, Serializable, Referenceable {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_TOPIC_EXCHANGE = "jms.topic";
    public static final String DEFAULT_DELAYED_QUEUE_EXCHANGE = "jms.delayed";
    public static final String DEFAULT_DELAYED_TOPIC_EXCHANGE = "jms.delayed.topic";

    private String host = "localhost";
    private int port = -1;
    private List<AmqpUri.Address> addresses;
    private boolean useTls;
    private String username = "guest";
    private String password = "guest";
    private String virtualHost = "/";
    private String clientID;
    private transient SSLContext sslContext;
    private int heartbeat = 60;
    private int connectionTimeout = 60_000;
    private int rpcTimeout = 60_000;
    private int prefetch = 100;
    private String topicExchange = DEFAULT_TOPIC_EXCHANGE;
    private String delayedQueueExchange = DEFAULT_DELAYED_QUEUE_EXCHANGE;
    private String delayedTopicExchange = DEFAULT_DELAYED_TOPIC_EXCHANGE;
    private PublisherConfirmMode publisherConfirms = PublisherConfirmMode.PERSISTENT;
    private long confirmTimeout = 30_000;
    private boolean autoDeclareQueues = true;
    private int dupsOkBatchSize = 10;
    private String objectMessageSerialFilter = ObjectMessageDeserializer.DEFAULT_FILTER;
    private String connectionNameSuffix;
    private boolean automaticRecovery = true;
    private long recoveryInterval = 2_000;
    private long recoveryMaxInterval = 30_000;
    private int recoveryAttempts;
    private long recoveryTimeout = 60_000;
    private transient RecoveryListener recoveryListener;

    public LavinMQConnectionFactory() {
    }

    public LavinMQConnectionFactory(String url) {
        setUrl(url);
    }

    // ---- URL --------------------------------------------------------------------------------------------------------

    /** Applies host(s), port, TLS, credentials, vhost and query parameters from an amqp:// or amqps:// URL. */
    public final void setUrl(String url) {
        AmqpUri uri = AmqpUri.parse(url);
        this.addresses = new ArrayList<>(uri.addresses());
        this.host = uri.host();
        this.port = uri.port();
        this.useTls = uri.tls();
        this.username = uri.username();
        this.password = uri.password();
        this.virtualHost = uri.vhost();
        for (Map.Entry<String, String> e : uri.query().entrySet()) applyProperty(e.getKey(), e.getValue());
    }

    /** Applies one configuration property by name (the bean property name or its snake_case form). */
    public final void applyProperty(String key, String value) {
        switch (key.replace("_", "").toLowerCase(java.util.Locale.ROOT)) {
            case "heartbeat": setHeartbeat(Integer.parseInt(value)); break;
            case "connectiontimeout": setConnectionTimeout(Integer.parseInt(value)); break;
            case "rpctimeout": setRpcTimeout(Integer.parseInt(value)); break;
            case "prefetch": setPrefetch(Integer.parseInt(value)); break;
            case "clientid": setClientID(value); break;
            case "topicexchange": setTopicExchange(value); break;
            case "delayedqueueexchange": setDelayedQueueExchange(value); break;
            case "delayedtopicexchange": setDelayedTopicExchange(value); break;
            case "publisherconfirms": setPublisherConfirms(PublisherConfirmMode.valueOf(value.toUpperCase(java.util.Locale.ROOT))); break;
            case "confirmtimeout": setConfirmTimeout(Long.parseLong(value)); break;
            case "autodeclarequeues": setAutoDeclareQueues(Boolean.parseBoolean(value)); break;
            case "dupsokbatchsize": setDupsOkBatchSize(Integer.parseInt(value)); break;
            case "objectmessageserialfilter": setObjectMessageSerialFilter(value); break;
            case "connectionnamesuffix": setConnectionNameSuffix(value); break;
            case "automaticrecovery": setAutomaticRecovery(Boolean.parseBoolean(value)); break;
            case "recoveryinterval": setRecoveryInterval(Long.parseLong(value)); break;
            case "recoverymaxinterval": setRecoveryMaxInterval(Long.parseLong(value)); break;
            case "recoveryattempts": setRecoveryAttempts(Integer.parseInt(value)); break;
            case "recoverytimeout": setRecoveryTimeout(Long.parseLong(value)); break;
            case "host": setHost(value); break;
            case "port": setPort(Integer.parseInt(value)); break;
            case "username": setUsername(value); break;
            case "password": setPassword(value); break;
            case "virtualhost": setVirtualHost(value); break;
            case "usetls": setUseTls(Boolean.parseBoolean(value)); break;
            case "addresses": setAddresses(value); break;
            default: throw new IllegalArgumentException("unknown configuration property: " + key);
        }
    }

    /** The addresses to try, in order; derived from host/port unless set explicitly or through a multi-host URL. */
    List<AmqpUri.Address> addresses() {
        if (addresses != null && !addresses.isEmpty()) return addresses;
        return List.of(new AmqpUri.Address(host, port > 0 ? port : (useTls ? AmqpUri.DEFAULT_TLS_PORT : AmqpUri.DEFAULT_PORT)));
    }

    // ---- bean properties ------------------------------------------------------------------------------------------

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; this.addresses = null; }
    public int getPort() { return port > 0 ? port : (useTls ? AmqpUri.DEFAULT_TLS_PORT : AmqpUri.DEFAULT_PORT); }
    public void setPort(int port) { this.port = port; this.addresses = null; }
    /** Comma-separated host[:port] list for cluster fail-over. */
    public String getAddresses() {
        StringBuilder sb = new StringBuilder();
        for (AmqpUri.Address a : addresses()) {
            if (sb.length() > 0) sb.append(',');
            sb.append(a);
        }
        return sb.toString();
    }
    public void setAddresses(String list) {
        List<AmqpUri.Address> out = new ArrayList<>();
        int def = useTls ? AmqpUri.DEFAULT_TLS_PORT : AmqpUri.DEFAULT_PORT;
        for (String s : list.split(",")) {
            if (s.isBlank()) continue;
            String hp = s.trim();
            AmqpUri.Address a = AmqpUri.parse("amqp://" + hp).addresses().get(0);
            boolean hasPort = hp.lastIndexOf(':') > hp.lastIndexOf(']');
            out.add(hasPort ? a : new AmqpUri.Address(a.host(), def));
        }
        this.addresses = out;
        if (!out.isEmpty()) {
            this.host = out.get(0).host();
            this.port = out.get(0).port();
        }
    }
    public boolean isUseTls() { return useTls; }
    public void setUseTls(boolean useTls) { this.useTls = useTls; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public String getVirtualHost() { return virtualHost; }
    public void setVirtualHost(String virtualHost) { this.virtualHost = virtualHost; }
    public String getClientID() { return clientID; }
    public void setClientID(String clientID) { this.clientID = clientID; }
    public SSLContext getSslContext() { return sslContext; }
    public void setSslContext(SSLContext sslContext) { this.sslContext = sslContext; }
    /** Heartbeat interval in seconds (0 disables). */
    public int getHeartbeat() { return heartbeat; }
    public void setHeartbeat(int heartbeat) { this.heartbeat = heartbeat; }
    /** TCP connect and handshake timeout in milliseconds. */
    public int getConnectionTimeout() { return connectionTimeout; }
    public void setConnectionTimeout(int connectionTimeout) { this.connectionTimeout = connectionTimeout; }
    /** Timeout for synchronous AMQP methods in milliseconds. */
    public int getRpcTimeout() { return rpcTimeout; }
    public void setRpcTimeout(int rpcTimeout) { this.rpcTimeout = rpcTimeout; }
    /** Unacknowledged messages the broker may push to a session (basic.qos). */
    public int getPrefetch() { return prefetch; }
    public void setPrefetch(int prefetch) { this.prefetch = prefetch; }
    public String getTopicExchange() { return topicExchange; }
    public void setTopicExchange(String topicExchange) { this.topicExchange = topicExchange; }
    public String getDelayedQueueExchange() { return delayedQueueExchange; }
    public void setDelayedQueueExchange(String delayedQueueExchange) { this.delayedQueueExchange = delayedQueueExchange; }
    public String getDelayedTopicExchange() { return delayedTopicExchange; }
    public void setDelayedTopicExchange(String delayedTopicExchange) { this.delayedTopicExchange = delayedTopicExchange; }
    public PublisherConfirmMode getPublisherConfirms() { return publisherConfirms; }
    public void setPublisherConfirms(PublisherConfirmMode publisherConfirms) { this.publisherConfirms = publisherConfirms; }
    /** How long a synchronous send waits for the publisher confirm, in milliseconds. */
    public long getConfirmTimeout() { return confirmTimeout; }
    public void setConfirmTimeout(long confirmTimeout) { this.confirmTimeout = confirmTimeout; }
    /** Declare a durable queue when a JMS queue does not exist yet (default true). */
    public boolean isAutoDeclareQueues() { return autoDeclareQueues; }
    public void setAutoDeclareQueues(boolean autoDeclareQueues) { this.autoDeclareQueues = autoDeclareQueues; }
    public int getDupsOkBatchSize() { return dupsOkBatchSize; }
    public void setDupsOkBatchSize(int dupsOkBatchSize) { this.dupsOkBatchSize = dupsOkBatchSize; }
    /** {@link java.io.ObjectInputFilter} pattern applied when deserializing ObjectMessage bodies. */
    public String getObjectMessageSerialFilter() { return objectMessageSerialFilter; }
    public void setObjectMessageSerialFilter(String objectMessageSerialFilter) { this.objectMessageSerialFilter = objectMessageSerialFilter; }
    /** Appended to the connection name shown in the management UI. */
    public String getConnectionNameSuffix() { return connectionNameSuffix; }
    public void setConnectionNameSuffix(String connectionNameSuffix) { this.connectionNameSuffix = connectionNameSuffix; }
    public boolean isAutomaticRecovery() { return automaticRecovery; }
    public void setAutomaticRecovery(boolean automaticRecovery) { this.automaticRecovery = automaticRecovery; }
    /** First delay between reconnection attempts in milliseconds; doubles up to {@link #getRecoveryMaxInterval()}. */
    public long getRecoveryInterval() { return recoveryInterval; }
    public void setRecoveryInterval(long recoveryInterval) { this.recoveryInterval = recoveryInterval; }
    public long getRecoveryMaxInterval() { return recoveryMaxInterval; }
    public void setRecoveryMaxInterval(long recoveryMaxInterval) { this.recoveryMaxInterval = recoveryMaxInterval; }
    /** Maximum reconnection attempts; 0 means unlimited. */
    public int getRecoveryAttempts() { return recoveryAttempts; }
    public void setRecoveryAttempts(int recoveryAttempts) { this.recoveryAttempts = recoveryAttempts; }
    /** How long sends and administrative calls wait for a recovery in progress, in milliseconds. */
    public long getRecoveryTimeout() { return recoveryTimeout; }
    public void setRecoveryTimeout(long recoveryTimeout) { this.recoveryTimeout = recoveryTimeout; }
    public RecoveryListener getRecoveryListener() { return recoveryListener; }
    public void setRecoveryListener(RecoveryListener recoveryListener) { this.recoveryListener = recoveryListener; }

    // ---- factory methods ------------------------------------------------------------------------------------------

    ConnectionSettings settings() {
        return new ConnectionSettings(this);
    }

    @Override
    public LavinMQConnection createConnection() throws JMSException {
        return LavinMQConnection.open(settings());
    }

    @Override
    public LavinMQConnection createConnection(String userName, String password) throws JMSException {
        return LavinMQConnection.open(new ConnectionSettings(settings(), userName, password));
    }

    @Override
    public JMSContext createContext() {
        return createContext(JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        try {
            return LavinMQJMSContext.create(createConnection(userName, password), sessionMode);
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        try {
            return LavinMQJMSContext.create(createConnection(), sessionMode);
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return createConnection(userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createConnection();
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return createConnection(userName, password);
    }

    // ---- JNDI -----------------------------------------------------------------------------------------------------

    @Override
    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(), LavinMQObjectFactory.class.getName(), null);
        ref.add(new StringRefAddr("addresses", getAddresses()));
        ref.add(new StringRefAddr("useTls", Boolean.toString(useTls)));
        ref.add(new StringRefAddr("username", username));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("virtualHost", virtualHost));
        if (clientID != null) ref.add(new StringRefAddr("clientID", clientID));
        ref.add(new StringRefAddr("heartbeat", Integer.toString(heartbeat)));
        ref.add(new StringRefAddr("connectionTimeout", Integer.toString(connectionTimeout)));
        ref.add(new StringRefAddr("rpcTimeout", Integer.toString(rpcTimeout)));
        ref.add(new StringRefAddr("prefetch", Integer.toString(prefetch)));
        ref.add(new StringRefAddr("topicExchange", topicExchange));
        ref.add(new StringRefAddr("delayedQueueExchange", delayedQueueExchange));
        ref.add(new StringRefAddr("delayedTopicExchange", delayedTopicExchange));
        ref.add(new StringRefAddr("publisherConfirms", publisherConfirms.name()));
        ref.add(new StringRefAddr("confirmTimeout", Long.toString(confirmTimeout)));
        ref.add(new StringRefAddr("autoDeclareQueues", Boolean.toString(autoDeclareQueues)));
        ref.add(new StringRefAddr("dupsOkBatchSize", Integer.toString(dupsOkBatchSize)));
        ref.add(new StringRefAddr("objectMessageSerialFilter", objectMessageSerialFilter));
        if (connectionNameSuffix != null) ref.add(new StringRefAddr("connectionNameSuffix", connectionNameSuffix));
        ref.add(new StringRefAddr("automaticRecovery", Boolean.toString(automaticRecovery)));
        ref.add(new StringRefAddr("recoveryInterval", Long.toString(recoveryInterval)));
        ref.add(new StringRefAddr("recoveryMaxInterval", Long.toString(recoveryMaxInterval)));
        ref.add(new StringRefAddr("recoveryAttempts", Integer.toString(recoveryAttempts)));
        ref.add(new StringRefAddr("recoveryTimeout", Long.toString(recoveryTimeout)));
        return ref;
    }

    @Override
    public String toString() {
        return "LavinMQConnectionFactory[" + (useTls ? "amqps" : "amqp") + "://" + username + "@" + getAddresses() + "/" + virtualHost + "]";
    }

    /** Builds the connection name sent to the broker. */
    String connectionName(String connectionId) {
        String base = "lavinmq-jms-" + connectionId;
        return connectionNameSuffix == null ? base : base + " " + connectionNameSuffix;
    }
}
