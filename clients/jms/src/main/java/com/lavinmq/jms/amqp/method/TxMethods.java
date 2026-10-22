package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.codec.AmqpWriter;

/** Class 90: tx. */
public final class TxMethods {
    public static final int CLASS_ID = 90;

    private TxMethods() {
    }

    public record Select() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "tx.select"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record SelectOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "tx.select-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Commit() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 20; }
        public String name() { return "tx.commit"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record CommitOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 21; }
        public String name() { return "tx.commit-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Rollback() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 30; }
        public String name() { return "tx.rollback"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record RollbackOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 31; }
        public String name() { return "tx.rollback-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId) {
        switch (methodId) {
            case 10: return new Select();
            case 11: return new SelectOk();
            case 20: return new Commit();
            case 21: return new CommitOk();
            case 30: return new Rollback();
            case 31: return new RollbackOk();
            default: return null;
        }
    }
}
