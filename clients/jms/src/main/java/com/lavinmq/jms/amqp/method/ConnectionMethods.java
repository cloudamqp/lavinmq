package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;
import com.lavinmq.jms.amqp.codec.AmqpWriter;
import java.util.Map;

/** Class 10: connection. */
public final class ConnectionMethods {
    public static final int CLASS_ID = 10;

    private ConnectionMethods() {
    }

    public record Start(int versionMajor, int versionMinor, Map<String, Object> serverProperties, String mechanisms, String locales) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 10; }
        public String name() { return "connection.start"; }
        public void writeArguments(AmqpWriter w) {
            w.octet(versionMajor).octet(versionMinor).table(serverProperties).longstr(mechanisms).longstr(locales);
        }
        static Start read(AmqpReader r) throws ProtocolException {
            return new Start(r.octet(), r.octet(), r.table(), r.longstrAsString(), r.longstrAsString());
        }
    }

    public record StartOk(Map<String, Object> clientProperties, String mechanism, byte[] response, String locale) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 11; }
        public String name() { return "connection.start-ok"; }
        public void writeArguments(AmqpWriter w) {
            w.table(clientProperties).shortstr(mechanism).longstr(response).shortstr(locale);
        }
        static StartOk read(AmqpReader r) throws ProtocolException {
            return new StartOk(r.table(), r.shortstr(), r.longstr(), r.shortstr());
        }
    }

    public record Tune(int channelMax, long frameMax, int heartbeat) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 30; }
        public String name() { return "connection.tune"; }
        public void writeArguments(AmqpWriter w) { w.short_(channelMax).long_(frameMax).short_(heartbeat); }
        static Tune read(AmqpReader r) throws ProtocolException { return new Tune(r.short_(), r.long_(), r.short_()); }
    }

    public record TuneOk(int channelMax, long frameMax, int heartbeat) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 31; }
        public String name() { return "connection.tune-ok"; }
        public void writeArguments(AmqpWriter w) { w.short_(channelMax).long_(frameMax).short_(heartbeat); }
        static TuneOk read(AmqpReader r) throws ProtocolException { return new TuneOk(r.short_(), r.long_(), r.short_()); }
    }

    public record Open(String virtualHost) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 40; }
        public String name() { return "connection.open"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(virtualHost).shortstr("").bits(false); }
        static Open read(AmqpReader r) throws ProtocolException {
            String vhost = r.shortstr();
            r.shortstr();
            r.bits(1);
            return new Open(vhost);
        }
    }

    public record OpenOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 41; }
        public String name() { return "connection.open-ok"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(""); }
        static OpenOk read(AmqpReader r) throws ProtocolException { r.shortstr(); return new OpenOk(); }
    }

    public record Close(int replyCode, String replyText, int classId_, int methodId_) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 50; }
        public String name() { return "connection.close"; }
        /** Class id of the method that caused the close (0 if none). */
        public int failingClassId() { return classId_; }
        public int failingMethodId() { return methodId_; }
        public void writeArguments(AmqpWriter w) { w.short_(replyCode).shortstr(replyText).short_(classId_).short_(methodId_); }
        static Close read(AmqpReader r) throws ProtocolException { return new Close(r.short_(), r.shortstr(), r.short_(), r.short_()); }
    }

    public record CloseOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 51; }
        public String name() { return "connection.close-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record Blocked(String reason) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 60; }
        public String name() { return "connection.blocked"; }
        public void writeArguments(AmqpWriter w) { w.shortstr(reason); }
        static Blocked read(AmqpReader r) throws ProtocolException { return new Blocked(r.shortstr()); }
    }

    public record Unblocked() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 61; }
        public String name() { return "connection.unblocked"; }
        public void writeArguments(AmqpWriter w) { }
    }

    public record UpdateSecret(byte[] secret, String reason) implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 70; }
        public String name() { return "connection.update-secret"; }
        public void writeArguments(AmqpWriter w) { w.longstr(secret).shortstr(reason); }
        static UpdateSecret read(AmqpReader r) throws ProtocolException { return new UpdateSecret(r.longstr(), r.shortstr()); }
    }

    public record UpdateSecretOk() implements Method {
        public int classId() { return CLASS_ID; }
        public int methodId() { return 71; }
        public String name() { return "connection.update-secret-ok"; }
        public void writeArguments(AmqpWriter w) { }
    }

    static Method decode(int methodId, AmqpReader r) throws ProtocolException {
        switch (methodId) {
            case 10: return Start.read(r);
            case 11: return StartOk.read(r);
            case 30: return Tune.read(r);
            case 31: return TuneOk.read(r);
            case 40: return Open.read(r);
            case 41: return OpenOk.read(r);
            case 50: return Close.read(r);
            case 51: return new CloseOk();
            case 60: return Blocked.read(r);
            case 61: return new Unblocked();
            case 70: return UpdateSecret.read(r);
            case 71: return new UpdateSecretOk();
            default: return null;
        }
    }
}
