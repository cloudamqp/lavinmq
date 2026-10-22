package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpUri;
import java.io.Serializable;
import java.util.List;
import javax.net.ssl.SSLContext;

/** Immutable snapshot of a connection factory's configuration, taken when a connection is created. */
public final class ConnectionSettings implements Serializable {
    private static final long serialVersionUID = 1L;

    final List<AmqpUri.Address> addresses;
    final boolean tls;
    final String username;
    final String password;
    final String virtualHost;
    final String clientId;
    final transient SSLContext sslContext;
    final int heartbeatSeconds;
    final int connectionTimeoutMs;
    final int rpcTimeoutMs;
    final int prefetch;
    final String topicExchange;
    final String delayedQueueExchange;
    final String delayedTopicExchange;
    final PublisherConfirmMode publisherConfirms;
    final long confirmTimeoutMs;
    final boolean autoDeclareQueues;
    final int dupsOkBatchSize;
    final String objectMessageSerialFilter;
    final String connectionNameSuffix;
    final boolean automaticRecovery;
    final long recoveryIntervalMs;
    final long recoveryMaxIntervalMs;
    final int recoveryAttempts;
    final long recoveryTimeoutMs;
    final transient RecoveryListener recoveryListener;

    ConnectionSettings(LavinMQConnectionFactory f) {
        this.addresses = List.copyOf(f.addresses());
        this.tls = f.isUseTls();
        this.username = f.getUsername();
        this.password = f.getPassword();
        this.virtualHost = f.getVirtualHost();
        this.clientId = f.getClientID();
        this.sslContext = f.getSslContext();
        this.heartbeatSeconds = f.getHeartbeat();
        this.connectionTimeoutMs = f.getConnectionTimeout();
        this.rpcTimeoutMs = f.getRpcTimeout();
        this.prefetch = f.getPrefetch();
        this.topicExchange = f.getTopicExchange();
        this.delayedQueueExchange = f.getDelayedQueueExchange();
        this.delayedTopicExchange = f.getDelayedTopicExchange();
        this.publisherConfirms = f.getPublisherConfirms();
        this.confirmTimeoutMs = f.getConfirmTimeout();
        this.autoDeclareQueues = f.isAutoDeclareQueues();
        this.dupsOkBatchSize = f.getDupsOkBatchSize();
        this.objectMessageSerialFilter = f.getObjectMessageSerialFilter();
        this.connectionNameSuffix = f.getConnectionNameSuffix();
        this.automaticRecovery = f.isAutomaticRecovery();
        this.recoveryIntervalMs = f.getRecoveryInterval();
        this.recoveryMaxIntervalMs = f.getRecoveryMaxInterval();
        this.recoveryAttempts = f.getRecoveryAttempts();
        this.recoveryTimeoutMs = f.getRecoveryTimeout();
        this.recoveryListener = f.getRecoveryListener();
    }

    ConnectionSettings(ConnectionSettings s, String username, String password) {
        this.addresses = s.addresses;
        this.tls = s.tls;
        this.username = username;
        this.password = password;
        this.virtualHost = s.virtualHost;
        this.clientId = s.clientId;
        this.sslContext = s.sslContext;
        this.heartbeatSeconds = s.heartbeatSeconds;
        this.connectionTimeoutMs = s.connectionTimeoutMs;
        this.rpcTimeoutMs = s.rpcTimeoutMs;
        this.prefetch = s.prefetch;
        this.topicExchange = s.topicExchange;
        this.delayedQueueExchange = s.delayedQueueExchange;
        this.delayedTopicExchange = s.delayedTopicExchange;
        this.publisherConfirms = s.publisherConfirms;
        this.confirmTimeoutMs = s.confirmTimeoutMs;
        this.autoDeclareQueues = s.autoDeclareQueues;
        this.dupsOkBatchSize = s.dupsOkBatchSize;
        this.objectMessageSerialFilter = s.objectMessageSerialFilter;
        this.connectionNameSuffix = s.connectionNameSuffix;
        this.automaticRecovery = s.automaticRecovery;
        this.recoveryIntervalMs = s.recoveryIntervalMs;
        this.recoveryMaxIntervalMs = s.recoveryMaxIntervalMs;
        this.recoveryAttempts = s.recoveryAttempts;
        this.recoveryTimeoutMs = s.recoveryTimeoutMs;
        this.recoveryListener = s.recoveryListener;
    }

    public List<AmqpUri.Address> addresses() { return addresses; }
    public String topicExchange() { return topicExchange; }
    public String delayedQueueExchange() { return delayedQueueExchange; }
    public String delayedTopicExchange() { return delayedTopicExchange; }
    public int prefetch() { return prefetch; }
    public PublisherConfirmMode publisherConfirms() { return publisherConfirms; }
    public long confirmTimeoutMs() { return confirmTimeoutMs; }
    public boolean autoDeclareQueues() { return autoDeclareQueues; }
    public int dupsOkBatchSize() { return dupsOkBatchSize; }
    public boolean automaticRecovery() { return automaticRecovery; }
    public long recoveryTimeoutMs() { return recoveryTimeoutMs; }
}
