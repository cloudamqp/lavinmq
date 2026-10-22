package com.lavinmq.jms.amqp;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;

/** Settings for opening one {@link AmqpConnection}. Build with {@link #builder()}. */
public final class ConnectionConfig {
    public static final int DEFAULT_FRAME_MAX = 131072;
    public static final int DEFAULT_CHANNEL_MAX = 2047;
    public static final String PRODUCT = "LavinMQ JMS Client";
    public static final String VERSION = loadVersion();

    private final String host;
    private final int port;
    private final boolean tls;
    private final String username;
    private final String password;
    private final String vhost;
    private final SSLContext sslContext;
    private final int connectionTimeoutMs;
    private final int heartbeatSeconds;
    private final int requestedFrameMax;
    private final int requestedChannelMax;
    private final int rpcTimeoutMs;
    private final String connectionName;
    private final Map<String, Object> clientPropertyOverrides;
    private final Consumer<Socket> socketConfigurator;
    private final ThreadFactory threadFactory;

    private ConnectionConfig(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.tls = b.tls;
        this.username = b.username;
        this.password = b.password;
        this.vhost = b.vhost;
        this.sslContext = b.sslContext;
        this.connectionTimeoutMs = b.connectionTimeoutMs;
        this.heartbeatSeconds = b.heartbeatSeconds;
        this.requestedFrameMax = b.requestedFrameMax;
        this.requestedChannelMax = b.requestedChannelMax;
        this.rpcTimeoutMs = b.rpcTimeoutMs;
        this.connectionName = b.connectionName;
        this.clientPropertyOverrides = new LinkedHashMap<>(b.clientPropertyOverrides);
        this.socketConfigurator = b.socketConfigurator;
        this.threadFactory = b.threadFactory;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String host() { return host; }
    public int port() { return port; }
    public boolean tls() { return tls; }
    public String username() { return username; }
    public String password() { return password; }
    public String vhost() { return vhost; }
    public SSLContext sslContext() { return sslContext; }
    public int connectionTimeoutMs() { return connectionTimeoutMs; }
    public int heartbeatSeconds() { return heartbeatSeconds; }
    public int requestedFrameMax() { return requestedFrameMax; }
    public int requestedChannelMax() { return requestedChannelMax; }
    public int rpcTimeoutMs() { return rpcTimeoutMs; }
    public String connectionName() { return connectionName; }
    public Consumer<Socket> socketConfigurator() { return socketConfigurator; }
    public ThreadFactory threadFactory() { return threadFactory; }

    /** The client-properties table sent in connection.start-ok. */
    public Map<String, Object> clientProperties() {
        Map<String, Object> caps = new LinkedHashMap<>();
        caps.put("publisher_confirms", true);
        caps.put("consumer_cancel_notify", true);
        caps.put("connection.blocked", true);
        caps.put("authentication_failure_close", true);
        caps.put("basic.nack", true);
        caps.put("exchange_exchange_bindings", true);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("product", PRODUCT);
        props.put("version", VERSION);
        props.put("platform", "Java " + Runtime.version());
        props.put("capabilities", caps);
        if (connectionName != null) props.put("connection_name", connectionName);
        props.putAll(clientPropertyOverrides);
        return props;
    }

    private static String loadVersion() {
        try (InputStream in = ConnectionConfig.class.getResourceAsStream("/com/lavinmq/jms/version.properties")) {
            if (in != null) {
                Properties p = new Properties();
                p.load(in);
                String v = p.getProperty("version");
                if (v != null && !v.startsWith("${")) return v;
            }
        } catch (IOException ignored) {
            // fall through
        }
        return "unknown";
    }

    public static final class Builder {
        private String host = "localhost";
        private int port = AmqpUri.DEFAULT_PORT;
        private boolean tls;
        private String username = "guest";
        private String password = "guest";
        private String vhost = "/";
        private SSLContext sslContext;
        private int connectionTimeoutMs = 60_000;
        private int heartbeatSeconds = 60;
        private int requestedFrameMax = DEFAULT_FRAME_MAX;
        private int requestedChannelMax = DEFAULT_CHANNEL_MAX;
        private int rpcTimeoutMs = 60_000;
        private String connectionName;
        private final Map<String, Object> clientPropertyOverrides = new LinkedHashMap<>();
        private Consumer<Socket> socketConfigurator = s -> {
            try {
                s.setTcpNoDelay(true);
                s.setKeepAlive(true);
            } catch (IOException ignored) {
                // best effort
            }
        };
        private ThreadFactory threadFactory;

        /** Applies host, port, tls, credentials, vhost and known query parameters from a URI. */
        public Builder uri(AmqpUri uri) {
            return address(uri.addresses().get(0), uri);
        }

        /** Like {@link #uri(AmqpUri)} but connects to the given address of the URI's list. */
        public Builder address(AmqpUri.Address address, AmqpUri uri) {
            this.host = address.host();
            this.port = address.port();
            this.tls = uri.tls();
            this.username = uri.username();
            this.password = uri.password();
            this.vhost = uri.vhost();
            Integer hb = uri.heartbeatSeconds();
            if (hb != null) this.heartbeatSeconds = hb;
            Integer ct = uri.connectionTimeoutMs();
            if (ct != null) this.connectionTimeoutMs = ct;
            return this;
        }

        public Builder uri(String uri) {
            return uri(AmqpUri.parse(uri));
        }

        public Builder host(String v) { host = v; return this; }
        public Builder port(int v) { port = v; return this; }
        public Builder tls(boolean v) { tls = v; return this; }
        public Builder username(String v) { username = v; return this; }
        public Builder password(String v) { password = v; return this; }
        public Builder vhost(String v) { vhost = v; return this; }
        public Builder sslContext(SSLContext v) { sslContext = v; return this; }
        public Builder connectionTimeoutMs(int v) { connectionTimeoutMs = v; return this; }
        public Builder heartbeatSeconds(int v) { heartbeatSeconds = v; return this; }
        public Builder requestedFrameMax(int v) { requestedFrameMax = v; return this; }
        public Builder requestedChannelMax(int v) { requestedChannelMax = v; return this; }
        public Builder rpcTimeoutMs(int v) { rpcTimeoutMs = v; return this; }
        public Builder connectionName(String v) { connectionName = v; return this; }
        public Builder clientProperty(String key, Object value) { clientPropertyOverrides.put(key, value); return this; }
        public Builder socketConfigurator(Consumer<Socket> v) { socketConfigurator = v; return this; }
        public Builder threadFactory(ThreadFactory v) { threadFactory = v; return this; }

        public ConnectionConfig build() {
            return new ConnectionConfig(this);
        }
    }
}
