package com.lavinmq.jms.amqp.method;

import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.codec.AmqpReader;

/** Decodes a method frame payload into a {@link Method}. */
public final class MethodDecoder {
    private MethodDecoder() {
    }

    public static Method decode(byte[] payload) throws ProtocolException {
        AmqpReader r = new AmqpReader(payload);
        int classId = r.short_();
        int methodId = r.short_();
        Method m;
        switch (classId) {
            case ConnectionMethods.CLASS_ID: m = ConnectionMethods.decode(methodId, r); break;
            case ChannelMethods.CLASS_ID: m = ChannelMethods.decode(methodId, r); break;
            case ExchangeMethods.CLASS_ID: m = ExchangeMethods.decode(methodId, r); break;
            case QueueMethods.CLASS_ID: m = QueueMethods.decode(methodId, r); break;
            case BasicMethods.CLASS_ID: m = BasicMethods.decode(methodId, r); break;
            case ConfirmMethods.CLASS_ID: m = ConfirmMethods.decode(methodId, r); break;
            case TxMethods.CLASS_ID: m = TxMethods.decode(methodId); break;
            default: m = null;
        }
        if (m == null) throw new ProtocolException("unknown method " + classId + "/" + methodId);
        return m;
    }
}
