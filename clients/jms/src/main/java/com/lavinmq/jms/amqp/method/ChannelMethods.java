package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;

/** Class 20: channel. */
public final class ChannelMethods {
    public static final int CLASS_ID = 20;

    private ChannelMethods() {
    }

    public record Open() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "channel.open"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(""); }
        static Open read(AmqpReader r) throws ProtocolException { r.shortstr(); return new Open(); }
    }

    public record OpenOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "channel.open-ok"; }
        public void writeArguments(AmqpWriter w) { w.longstr(new byte[0]); }
        static OpenOk read(AmqpReader r) throws ProtocolException { r.longstr(); return new OpenOk(); }
    }

    public record Flow(boolean active) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 20; }
        public String name() { return "channel.flow"; }
        public void writeArguments(AmqpWriter w) { w.bits(active); }
        static Flow read(AmqpReader r) throws ProtocolException { return new Flow(r.bits(1)[0]); }
    }

    public record FlowOk(boolean active) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 21; }
        public String name() { return "channel.flow-ok"; }
        public void writeArguments(AmqpWriter w) { w.bits(active); }
        static FlowOk read(AmqpReader r) throws ProtocolException { return new FlowOk(r.bits(1)[0]); }
    }

    public record Close(int replyCode, String replyText, int classId_, int methodId_) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 40; }
        public String name() { return "channel.close"; }
        public int failingClassId() { return classId_; }
        public int failingMethodId() { return methodId_; }
        public void writeArguments(AmqpWriter w) { w.short_(replyCode).shortstr(replyText).short_(classId_).short_(methodId_); }
        static Close read(AmqpReader r) throws ProtocolException { return new Close(r.short_(), r.shortstr(), r.short_(), r.short_()); }
    }

    public record CloseOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 41; }
        public String name() { return "channel.close-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Open.read(r);
            case 11: return OpenOk.read(r);
            case 20: return Flow.read(r);
            case 21: return FlowOk.read(r);
            case 40: return Close.read(r);
            case 41: return new CloseOk();
            default: return null;
        }
    }
}
