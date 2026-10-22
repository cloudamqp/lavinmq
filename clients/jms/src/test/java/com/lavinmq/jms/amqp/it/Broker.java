package com.lavinmq.jms.amqp.it;

import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.AmqpUri;
import com.lavinmq.jms.amqp.ConnectionConfig;
import java.io.IOException;
import java.util.UUID;

/** Access to the LavinMQ instance under test (env LAVINMQ_URL, default amqp://guest:guest@localhost:5672/%2F). */
public final class Broker {
    public static final String URL = System.getenv().getOrDefault("LAVINMQ_URL", "amqp://guest:guest@localhost:5672/%2F");
    public static final AmqpUri URI = AmqpUri.parse(URL);

    private Broker() {
    }

    public static ConnectionConfig.Builder config() {
        return ConnectionConfig.builder().uri(URI).heartbeatSeconds(10).rpcTimeoutMs(10_000).connectionTimeoutMs(10_000);
    }

    public static AmqpConnection connect() throws IOException {
        return AmqpConnection.open(config().build());
    }

    public static String uniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public static String httpUrl() {
        return System.getenv().getOrDefault("LAVINMQ_HTTP_URL", "http://" + URI.host() + ":15672");
    }
}
