package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;
import java.util.Map;

/** Class 50: queue. */
public final class QueueMethods {
    public static final int CLASS_ID = 50;

    private QueueMethods() {
    }

    public record Declare(String queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete,
                          boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "queue.declare"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(queue).bits(passive, durable, exclusive, autoDelete, noWait).table(arguments);
        }
        static Declare read(AmqpReader r) throws ProtocolException {
            r.short_();
            String queue = r.shortstr();
            boolean[] b = r.bits(5);
            return new Declare(queue, b[0], b[1], b[2], b[3], b[4], r.table());
        }
    }

    public record DeclareOk(String queue, long messageCount, long consumerCount) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "queue.declare-ok"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(queue).long_(messageCount).long_(consumerCount); }
        static DeclareOk read(AmqpReader r) throws ProtocolException { return new DeclareOk(r.shortstr(), r.long_(), r.long_()); }
    }

    public record Bind(String queue, String exchange, String routingKey, boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 20; }
        public String name() { return "queue.bind"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(queue).shortstr(exchange).shortstr(routingKey).bits(noWait).table(arguments);
        }
        static Bind read(AmqpReader r) throws ProtocolException {
            r.short_();
            return new Bind(r.shortstr(), r.shortstr(), r.shortstr(), r.bits(1)[0], r.table());
        }
    }

    public record BindOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 21; }
        public String name() { return "queue.bind-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Purge(String queue, boolean noWait) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 30; }
        public String name() { return "queue.purge"; }
        public void writeArguments(AmqpWriter w) { w.short_(0).shortstr(queue).bits(noWait); }
        static Purge read(AmqpReader r) throws ProtocolException { r.short_(); return new Purge(r.shortstr(), r.bits(1)[0]); }
    }

    public record PurgeOk(long messageCount) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 31; }
        public String name() { return "queue.purge-ok"; }
        public void writeArguments(AmqpWriter w) { w.long_(messageCount); }
        static PurgeOk read(AmqpReader r) throws ProtocolException { return new PurgeOk(r.long_()); }
    }

    public record Delete(String queue, boolean ifUnused, boolean ifEmpty, boolean noWait) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 40; }
        public String name() { return "queue.delete"; }
        public void writeArguments(AmqpWriter w) { w.short_(0).shortstr(queue).bits(ifUnused, ifEmpty, noWait); }
        static Delete read(AmqpReader r) throws ProtocolException {
            r.short_();
            String queue = r.shortstr();
            boolean[] b = r.bits(3);
            return new Delete(queue, b[0], b[1], b[2]);
        }
    }

    public record DeleteOk(long messageCount) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 41; }
        public String name() { return "queue.delete-ok"; }
        public void writeArguments(AmqpWriter w) { w.long_(messageCount); }
        static DeleteOk read(AmqpReader r) throws ProtocolException { return new DeleteOk(r.long_()); }
    }

    public record Unbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 50; }
        public String name() { return "queue.unbind"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(queue).shortstr(exchange).shortstr(routingKey).table(arguments);
        }
        static Unbind read(AmqpReader r) throws ProtocolException {
            r.short_();
            return new Unbind(r.shortstr(), r.shortstr(), r.shortstr(), r.table());
        }
    }

    public record UnbindOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 51; }
        public String name() { return "queue.unbind-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Declare.read(r);
            case 11: return DeclareOk.read(r);
            case 20: return Bind.read(r);
            case 21: return new BindOk();
            case 30: return Purge.read(r);
            case 31: return PurgeOk.read(r);
            case 40: return Delete.read(r);
            case 41: return DeleteOk.read(r);
            case 50: return Unbind.read(r);
            case 51: return new UnbindOk();
            default: return null;
        }
    }
}
