package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;

/** Class 85: confirm. */
public final class ConfirmMethods {
    public static final int CLASS_ID = 85;

    private ConfirmMethods() {
    }

    public record Select(boolean noWait) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "confirm.select"; }
        public void writeArguments(AmqpWriter w) { w.bits(noWait); }
        static Select read(AmqpReader r) throws ProtocolException { return new Select(r.bits(1)[0]); }
    }

    public record SelectOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "confirm.select-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Select.read(r);
            case 11: return new SelectOk();
            default: return null;
        }
    }
}
