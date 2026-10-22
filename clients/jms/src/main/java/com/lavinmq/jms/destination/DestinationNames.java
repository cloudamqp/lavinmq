package com.lavinmq.jms.destination;

import jakarta.jms.InvalidDestinationException;
import java.nio.charset.StandardCharsets;

/** Naming conventions shared by the client: validation, subscription queue names, temporary names and reply-to encoding. */
public final class DestinationNames {
    public static final String TEMP_QUEUE_PREFIX = "amq.gen-";
    public static final String TEMP_TOPIC_PREFIX = "jms.tmp.";
    public static final String DURABLE_PREFIX = "jms.durable.";
    public static final String SHARED_PREFIX = "jms.shared.";

    /** Values of the JMSReplyToType header. */
    public static final String TYPE_QUEUE = "queue";
    public static final String TYPE_TOPIC = "topic";
    public static final String TYPE_TEMP_QUEUE = "temp-queue";
    public static final String TYPE_TEMP_TOPIC = "temp-topic";

    private DestinationNames() {
    }

    /** AMQP names must be printable ASCII of at most 255 bytes. */
    public static String validate(String name, String what) throws InvalidDestinationException {
        if (name == null || name.isEmpty()) throw new InvalidDestinationException(what + " name must not be empty");
        if (name.getBytes(StandardCharsets.UTF_8).length > 255) throw new InvalidDestinationException(what + " name longer than 255 bytes");
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c < 0x21 || c > 0x7E) {
                throw new InvalidDestinationException(what + " name contains a character outside printable ASCII: " + name);
            }
        }
        return name;
    }

    public static boolean isTempQueueName(String name) {
        return name.startsWith(TEMP_QUEUE_PREFIX);
    }

    public static boolean isTempTopicName(String name) {
        return name.startsWith(TEMP_TOPIC_PREFIX);
    }

    public static String tempTopicName(String connectionId, long seq) {
        return TEMP_TOPIC_PREFIX + connectionId + "." + seq;
    }

    static String tempTopicOwner(String name) {
        if (!isTempTopicName(name)) return null;
        String rest = name.substring(TEMP_TOPIC_PREFIX.length());
        int dot = rest.lastIndexOf('.');
        return dot < 0 ? null : rest.substring(0, dot);
    }

    /** Queue name of a durable (shared or unshared) subscription. */
    public static String durableQueue(String clientId, String subscriptionName) {
        return DURABLE_PREFIX + encode(clientId == null ? "" : clientId) + "." + encode(subscriptionName);
    }

    /** Queue name of a non-durable shared subscription. */
    public static String sharedQueue(String clientId, String subscriptionName) {
        return SHARED_PREFIX + encode(clientId == null ? "" : clientId) + "." + encode(subscriptionName);
    }

    /** Percent-encodes anything outside the printable ASCII range, plus '%' and '.' (the separator). */
    static String encode(String s) {
        StringBuilder sb = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xFF;
            if (c > 0x21 && c < 0x7F && c != '%' && c != '.') sb.append((char) c);
            else sb.append('%').append(String.format("%02X", c));
        }
        return sb.toString();
    }

    public static String replyToType(LavinMQDestination d) {
        switch (d.kind()) {
            case QUEUE: return TYPE_QUEUE;
            case TOPIC: return TYPE_TOPIC;
            case TEMP_QUEUE: return TYPE_TEMP_QUEUE;
            case TEMP_TOPIC: return TYPE_TEMP_TOPIC;
            default: throw new IllegalStateException();
        }
    }

    /** Rebuilds a destination from a reply_to property and the optional JMSReplyToType header. */
    public static LavinMQDestination fromReplyTo(String name, String type) {
        if (type == null) {
            if (isTempQueueName(name)) return new LavinMQTemporaryQueue(name);
            if (isTempTopicName(name)) return new LavinMQTemporaryTopic(name);
            return new LavinMQQueue(name);
        }
        switch (type) {
            case TYPE_TOPIC: return new LavinMQTopic(name);
            case TYPE_TEMP_QUEUE: return new LavinMQTemporaryQueue(name);
            case TYPE_TEMP_TOPIC: return new LavinMQTemporaryTopic(name);
            default: return new LavinMQQueue(name);
        }
    }

    /** Derives the JMSDestination of a received message from the exchange and routing key it was delivered with. */
    public static LavinMQDestination fromDelivery(String exchange, String routingKey, String topicExchange, String delayedQueueExchange,
                                                  String delayedTopicExchange) {
        if (exchange.isEmpty() || exchange.equals(delayedQueueExchange)) {
            return isTempQueueName(routingKey) ? new LavinMQTemporaryQueue(routingKey) : new LavinMQQueue(routingKey);
        }
        if (exchange.equals(topicExchange) || exchange.equals(delayedTopicExchange)) {
            return isTempTopicName(routingKey) ? new LavinMQTemporaryTopic(routingKey) : new LavinMQTopic(routingKey);
        }
        return new LavinMQTopic(routingKey, exchange);
    }
}
