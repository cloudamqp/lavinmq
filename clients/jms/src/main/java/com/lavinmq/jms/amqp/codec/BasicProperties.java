package com.lavinmq.jms.amqp.codec;

import com.lavinmq.jms.amqp.ProtocolException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable AMQP 0-9-1 Basic content properties. */
public final class BasicProperties {
    public static final BasicProperties EMPTY = new Builder().build();

    private static final int F_CONTENT_TYPE = 0x8000;
    private static final int F_CONTENT_ENCODING = 0x4000;
    private static final int F_HEADERS = 0x2000;
    private static final int F_DELIVERY_MODE = 0x1000;
    private static final int F_PRIORITY = 0x0800;
    private static final int F_CORRELATION_ID = 0x0400;
    private static final int F_REPLY_TO = 0x0200;
    private static final int F_EXPIRATION = 0x0100;
    private static final int F_MESSAGE_ID = 0x0080;
    private static final int F_TIMESTAMP = 0x0040;
    private static final int F_TYPE = 0x0020;
    private static final int F_USER_ID = 0x0010;
    private static final int F_APP_ID = 0x0008;
    private static final int F_CLUSTER_ID = 0x0004;

    private final String contentType;
    private final String contentEncoding;
    private final Map<String, Object> headers;
    private final Integer deliveryMode;
    private final Integer priority;
    private final String correlationId;
    private final String replyTo;
    private final String expiration;
    private final String messageId;
    private final Long timestamp;
    private final String type;
    private final String userId;
    private final String appId;
    private final String clusterId;

    private BasicProperties(Builder b) {
        this.contentType = b.contentType;
        this.contentEncoding = b.contentEncoding;
        this.headers = b.headers == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(b.headers));
        this.deliveryMode = b.deliveryMode;
        this.priority = b.priority;
        this.correlationId = b.correlationId;
        this.replyTo = b.replyTo;
        this.expiration = b.expiration;
        this.messageId = b.messageId;
        this.timestamp = b.timestamp;
        this.type = b.type;
        this.userId = b.userId;
        this.appId = b.appId;
        this.clusterId = b.clusterId;
    }

    public String contentType() { return contentType; }
    public String contentEncoding() { return contentEncoding; }
    /** Unmodifiable headers table, or null when absent. */
    public Map<String, Object> headers() { return headers; }
    public Integer deliveryMode() { return deliveryMode; }
    public Integer priority() { return priority; }
    public String correlationId() { return correlationId; }
    public String replyTo() { return replyTo; }
    public String expiration() { return expiration; }
    public String messageId() { return messageId; }
    /** Seconds since the epoch. */
    public Long timestamp() { return timestamp; }
    public String type() { return type; }
    public String userId() { return userId; }
    public String appId() { return appId; }
    public String clusterId() { return clusterId; }

    public Builder builder() {
        Builder b = new Builder();
        b.contentType = contentType;
        b.contentEncoding = contentEncoding;
        b.headers = headers == null ? null : new LinkedHashMap<>(headers);
        b.deliveryMode = deliveryMode;
        b.priority = priority;
        b.correlationId = correlationId;
        b.replyTo = replyTo;
        b.expiration = expiration;
        b.messageId = messageId;
        b.timestamp = timestamp;
        b.type = type;
        b.userId = userId;
        b.appId = appId;
        b.clusterId = clusterId;
        return b;
    }

    public int flags() {
        int f = 0;
        if (contentType != null) f |= F_CONTENT_TYPE;
        if (contentEncoding != null) f |= F_CONTENT_ENCODING;
        if (headers != null) f |= F_HEADERS;
        if (deliveryMode != null) f |= F_DELIVERY_MODE;
        if (priority != null) f |= F_PRIORITY;
        if (correlationId != null) f |= F_CORRELATION_ID;
        if (replyTo != null) f |= F_REPLY_TO;
        if (expiration != null) f |= F_EXPIRATION;
        if (messageId != null) f |= F_MESSAGE_ID;
        if (timestamp != null) f |= F_TIMESTAMP;
        if (type != null) f |= F_TYPE;
        if (userId != null) f |= F_USER_ID;
        if (appId != null) f |= F_APP_ID;
        if (clusterId != null) f |= F_CLUSTER_ID;
        return f;
    }

    public void write(AmqpWriter w) {
        w.short_(flags());
        if (contentType != null) w.shortstr(contentType);
        if (contentEncoding != null) w.shortstr(contentEncoding);
        if (headers != null) w.table(headers);
        if (deliveryMode != null) w.octet(deliveryMode);
        if (priority != null) w.octet(priority);
        if (correlationId != null) w.shortstr(correlationId);
        if (replyTo != null) w.shortstr(replyTo);
        if (expiration != null) w.shortstr(expiration);
        if (messageId != null) w.shortstr(messageId);
        if (timestamp != null) w.timestamp(timestamp);
        if (type != null) w.shortstr(type);
        if (userId != null) w.shortstr(userId);
        if (appId != null) w.shortstr(appId);
        if (clusterId != null) w.shortstr(clusterId);
    }

    public static BasicProperties read(AmqpReader r) throws ProtocolException {
        int f = r.short_();
        if ((f & 0x0003) != 0) throw new ProtocolException("invalid basic property flags: 0x" + Integer.toHexString(f));
        Builder b = new Builder();
        if ((f & F_CONTENT_TYPE) != 0) b.contentType = r.shortstr();
        if ((f & F_CONTENT_ENCODING) != 0) b.contentEncoding = r.shortstr();
        if ((f & F_HEADERS) != 0) b.headers = r.table();
        if ((f & F_DELIVERY_MODE) != 0) b.deliveryMode = r.octet();
        if ((f & F_PRIORITY) != 0) b.priority = r.octet();
        if ((f & F_CORRELATION_ID) != 0) b.correlationId = r.shortstr();
        if ((f & F_REPLY_TO) != 0) b.replyTo = r.shortstr();
        if ((f & F_EXPIRATION) != 0) b.expiration = r.shortstr();
        if ((f & F_MESSAGE_ID) != 0) b.messageId = r.shortstr();
        if ((f & F_TIMESTAMP) != 0) b.timestamp = r.timestamp();
        if ((f & F_TYPE) != 0) b.type = r.shortstr();
        if ((f & F_USER_ID) != 0) b.userId = r.shortstr();
        if ((f & F_APP_ID) != 0) b.appId = r.shortstr();
        if ((f & F_CLUSTER_ID) != 0) b.clusterId = r.shortstr();
        return b.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BasicProperties)) return false;
        BasicProperties p = (BasicProperties) o;
        return Objects.equals(contentType, p.contentType) && Objects.equals(contentEncoding, p.contentEncoding)
            && Objects.equals(headers, p.headers) && Objects.equals(deliveryMode, p.deliveryMode)
            && Objects.equals(priority, p.priority) && Objects.equals(correlationId, p.correlationId)
            && Objects.equals(replyTo, p.replyTo) && Objects.equals(expiration, p.expiration)
            && Objects.equals(messageId, p.messageId) && Objects.equals(timestamp, p.timestamp)
            && Objects.equals(type, p.type) && Objects.equals(userId, p.userId)
            && Objects.equals(appId, p.appId) && Objects.equals(clusterId, p.clusterId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo,
            expiration, messageId, timestamp, type, userId, appId, clusterId);
    }

    @Override
    public String toString() {
        return "BasicProperties{contentType=" + contentType + ", deliveryMode=" + deliveryMode + ", priority=" + priority
            + ", correlationId=" + correlationId + ", replyTo=" + replyTo + ", expiration=" + expiration
            + ", messageId=" + messageId + ", timestamp=" + timestamp + ", type=" + type + ", headers=" + headers + '}';
    }

    public static final class Builder {
        private String contentType;
        private String contentEncoding;
        private Map<String, Object> headers;
        private Integer deliveryMode;
        private Integer priority;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private Long timestamp;
        private String type;
        private String userId;
        private String appId;
        private String clusterId;

        public Builder contentType(String v) { contentType = v; return this; }
        public Builder contentEncoding(String v) { contentEncoding = v; return this; }
        public Builder headers(Map<String, Object> v) { headers = v; return this; }
        public Builder deliveryMode(Integer v) { deliveryMode = v; return this; }
        public Builder priority(Integer v) { priority = v; return this; }
        public Builder correlationId(String v) { correlationId = v; return this; }
        public Builder replyTo(String v) { replyTo = v; return this; }
        public Builder expiration(String v) { expiration = v; return this; }
        public Builder messageId(String v) { messageId = v; return this; }
        public Builder timestamp(Long v) { timestamp = v; return this; }
        public Builder type(String v) { type = v; return this; }
        public Builder userId(String v) { userId = v; return this; }
        public Builder appId(String v) { appId = v; return this; }
        public Builder clusterId(String v) { clusterId = v; return this; }

        public Map<String, Object> headers() { return headers; }

        public BasicProperties build() {
            return new BasicProperties(this);
        }
    }
}
