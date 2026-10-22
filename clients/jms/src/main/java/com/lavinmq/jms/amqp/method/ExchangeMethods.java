package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;
import java.util.Map;

/** Class 40: exchange. */
public final class ExchangeMethods {
    public static final int CLASS_ID = 40;

    private ExchangeMethods() {
    }

    public record Declare(String exchange, String type, boolean passive, boolean durable, boolean autoDelete,
                          boolean internal, boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "exchange.declare"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(exchange).shortstr(type).bits(passive, durable, autoDelete, internal, noWait).table(arguments);
        }
        static Declare read(AmqpReader r) throws ProtocolException {
            r.short_();
            String exchange = r.shortstr();
            String type = r.shortstr();
            boolean[] b = r.bits(5);
            return new Declare(exchange, type, b[0], b[1], b[2], b[3], b[4], r.table());
        }
    }

    public record DeclareOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "exchange.declare-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Delete(String exchange, boolean ifUnused, boolean noWait) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 20; }
        public String name() { return "exchange.delete"; }
        public void writeArguments(AmqpWriter w) { w.short_(0).shortstr(exchange).bits(ifUnused, noWait); }
        static Delete read(AmqpReader r) throws ProtocolException {
            r.short_();
            String exchange = r.shortstr();
            boolean[] b = r.bits(2);
            return new Delete(exchange, b[0], b[1]);
        }
    }

    public record DeleteOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 21; }
        public String name() { return "exchange.delete-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Bind(String destination, String source, String routingKey, boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 30; }
        public String name() { return "exchange.bind"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(destination).shortstr(source).shortstr(routingKey).bits(noWait).table(arguments);
        }
        static Bind read(AmqpReader r) throws ProtocolException {
            r.short_();
            return new Bind(r.shortstr(), r.shortstr(), r.shortstr(), r.bits(1)[0], r.table());
        }
    }

    public record BindOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 31; }
        public String name() { return "exchange.bind-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Unbind(String destination, String source, String routingKey, boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 40; }
        public String name() { return "exchange.unbind"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(destination).shortstr(source).shortstr(routingKey).bits(noWait).table(arguments);
        }
        static Unbind read(AmqpReader r) throws ProtocolException {
            r.short_();
            return new Unbind(r.shortstr(), r.shortstr(), r.shortstr(), r.bits(1)[0], r.table());
        }
    }

    public record UnbindOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 51; }
        public String name() { return "exchange.unbind-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Declare.read(r);
            case 11: return new DeclareOk();
            case 20: return Delete.read(r);
            case 21: return new DeleteOk();
            case 30: return Bind.read(r);
            case 31: return new BindOk();
            case 40: return Unbind.read(r);
            case 51: return new UnbindOk();
            default: return null;
        }
    }
}
