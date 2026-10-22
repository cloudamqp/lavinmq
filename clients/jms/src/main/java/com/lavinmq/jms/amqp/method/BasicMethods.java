package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;
import java.util.Map;

/** Class 60: basic. */
public final class BasicMethods {
    public static final int CLASS_ID = 60;

    private BasicMethods() {
    }

    public record Qos(long prefetchSize, int prefetchCount, boolean global) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "basic.qos"; }
        public void writeArguments(AmqpWriter w) { w.long_(prefetchSize).short_(prefetchCount).bits(global); }
        static Qos read(AmqpReader r) throws ProtocolException { return new Qos(r.long_(), r.short_(), r.bits(1)[0]); }
    }

    public record QosOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "basic.qos-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Consume(String queue, String consumerTag, boolean noLocal, boolean noAck, boolean exclusive,
                          boolean noWait, Map<String, Object> arguments) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 20; }
        public String name() { return "basic.consume"; }
        public void writeArguments(AmqpWriter w) {
            w.short_(0).shortstr(queue).shortstr(consumerTag).bits(noLocal, noAck, exclusive, noWait).table(arguments);
        }
        static Consume read(AmqpReader r) throws ProtocolException {
            r.short_();
            String queue = r.shortstr();
            String tag = r.shortstr();
            boolean[] b = r.bits(4);
            return new Consume(queue, tag, b[0], b[1], b[2], b[3], r.table());
        }
    }

    public record ConsumeOk(String consumerTag) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 21; }
        public String name() { return "basic.consume-ok"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(consumerTag); }
        static ConsumeOk read(AmqpReader r) throws ProtocolException { return new ConsumeOk(r.shortstr()); }
    }

    public record Cancel(String consumerTag, boolean noWait) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 30; }
        public String name() { return "basic.cancel"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(consumerTag).bits(noWait); }
        static Cancel read(AmqpReader r) throws ProtocolException { return new Cancel(r.shortstr(), r.bits(1)[0]); }
    }

    public record CancelOk(String consumerTag) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 31; }
        public String name() { return "basic.cancel-ok"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(consumerTag); }
        static CancelOk read(AmqpReader r) throws ProtocolException { return new CancelOk(r.shortstr()); }
    }

    public record Publish(String exchange, String routingKey, boolean mandatory, boolean immediate) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 40; }
        public String name() { return "basic.publish"; }
        public boolean hasContent() { return true; }
        public void writeArguments(AmqpWriter w) { w.short_(0).shortstr(exchange).shortstr(routingKey).bits(mandatory, immediate); }
        static Publish read(AmqpReader r) throws ProtocolException {
            r.short_();
            String exchange = r.shortstr();
            String rk = r.shortstr();
            boolean[] b = r.bits(2);
            return new Publish(exchange, rk, b[0], b[1]);
        }
    }

    public record Return(int replyCode, String replyText, String exchange, String routingKey) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 50; }
        public String name() { return "basic.return"; }
        public boolean hasContent() { return true; }
        public void writeArguments(AmqpWriter w) { w.short_(replyCode).shortstr(replyText).shortstr(exchange).shortstr(routingKey); }
        static Return read(AmqpReader r) throws ProtocolException { return new Return(r.short_(), r.shortstr(), r.shortstr(), r.shortstr()); }
    }

    public record Deliver(String consumerTag, long deliveryTag, boolean redelivered, String exchange, String routingKey) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 60; }
        public String name() { return "basic.deliver"; }
        public boolean hasContent() { return true; }
        public void writeArguments(AmqpWriter w) {
            w.shortstr(consumerTag).longlong(deliveryTag).bits(redelivered).shortstr(exchange).shortstr(routingKey);
        }
        static Deliver read(AmqpReader r) throws ProtocolException {
            return new Deliver(r.shortstr(), r.longlong(), r.bits(1)[0], r.shortstr(), r.shortstr());
        }
    }

    public record Get(String queue, boolean noAck) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 70; }
        public String name() { return "basic.get"; }
        public void writeArguments(AmqpWriter w) { w.short_(0).shortstr(queue).bits(noAck); }
        static Get read(AmqpReader r) throws ProtocolException { r.short_(); return new Get(r.shortstr(), r.bits(1)[0]); }
    }

    public record GetOk(long deliveryTag, boolean redelivered, String exchange, String routingKey, long messageCount) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 71; }
        public String name() { return "basic.get-ok"; }
        public boolean hasContent() { return true; }
        public void writeArguments(AmqpWriter w) {
            w.longlong(deliveryTag).bits(redelivered).shortstr(exchange).shortstr(routingKey).long_(messageCount);
        }
        static GetOk read(AmqpReader r) throws ProtocolException {
            return new GetOk(r.longlong(), r.bits(1)[0], r.shortstr(), r.shortstr(), r.long_());
        }
    }

    public record GetEmpty() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 72; }
        public String name() { return "basic.get-empty"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(""); }
        static GetEmpty read(AmqpReader r) throws ProtocolException { r.shortstr(); return new GetEmpty(); }
    }

    public record Ack(long deliveryTag, boolean multiple) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 80; }
        public String name() { return "basic.ack"; }
        public void writeArguments(AmqpWriter w) { w.longlong(deliveryTag).bits(multiple); }
        static Ack read(AmqpReader r) throws ProtocolException { return new Ack(r.longlong(), r.bits(1)[0]); }
    }

    public record Reject(long deliveryTag, boolean requeue) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 90; }
        public String name() { return "basic.reject"; }
        public void writeArguments(AmqpWriter w) { w.longlong(deliveryTag).bits(requeue); }
        static Reject read(AmqpReader r) throws ProtocolException { return new Reject(r.longlong(), r.bits(1)[0]); }
    }

    public record Recover(boolean requeue) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 110; }
        public String name() { return "basic.recover"; }
        public void writeArguments(AmqpWriter w) { w.bits(requeue); }
        static Recover read(AmqpReader r) throws ProtocolException { return new Recover(r.bits(1)[0]); }
    }

    public record RecoverOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 111; }
        public String name() { return "basic.recover-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Nack(long deliveryTag, boolean multiple, boolean requeue) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 120; }
        public String name() { return "basic.nack"; }
        public void writeArguments(AmqpWriter w) { w.longlong(deliveryTag).bits(multiple, requeue); }
        static Nack read(AmqpReader r) throws ProtocolException {
            long tag = r.longlong();
            boolean[] b = r.bits(2);
            return new Nack(tag, b[0], b[1]);
        }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Qos.read(r);
            case 11: return new QosOk();
            case 20: return Consume.read(r);
            case 21: return ConsumeOk.read(r);
            case 30: return Cancel.read(r);
            case 31: return CancelOk.read(r);
            case 40: return Publish.read(r);
            case 50: return Return.read(r);
            case 60: return Deliver.read(r);
            case 70: return Get.read(r);
            case 71: return GetOk.read(r);
            case 72: return GetEmpty.read(r);
            case 80: return Ack.read(r);
            case 90: return Reject.read(r);
            case 110: return Recover.read(r);
            case 111: return new RecoverOk();
            case 120: return Nack.read(r);
            default: return null;
        }
    }
}
