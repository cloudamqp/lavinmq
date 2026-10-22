package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.Delivery;
import com.lavinmq.jms.amqp.GetResponse;
import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.ReturnedMessage;
import com.lavinmq.jms.amqp.codec.ContentHeader;
import com.lavinmq.jms.amqp.method.BasicMethods;
import com.lavinmq.jms.amqp.method.Method;

/** Collects a content-bearing method, its header and body frames into one message. Reader thread only. */
public final class ContentAssembler {
    private Method pending;
    private ContentHeader header;
    private byte[] body;
    private int received;

    public void onMethod(Method m) throws ProtocolException {
        if (pending != null) throw new ProtocolException("received " + m.name() + " while content of " + pending.name() + " was incomplete");
        pending = m;
        header = null;
        body = null;
        received = 0;
    }

    /** Returns true when the message is complete (empty body). */
    public boolean onHeader(ContentHeader h) throws ProtocolException {
        if (pending == null) throw new ProtocolException("content header without a preceding method");
        if (header != null) throw new ProtocolException("duplicate content header");
        if (h.bodySize() > Integer.MAX_VALUE - 16) throw new ProtocolException("body too large: " + h.bodySize());
        header = h;
        body = new byte[(int) h.bodySize()];
        received = 0;
        return h.bodySize() == 0;
    }

    /** Returns true when the message is complete. */
    public boolean onBody(byte[] chunk) throws ProtocolException {
        if (header == null) throw new ProtocolException("content body without a preceding header");
        if (received + chunk.length > body.length) throw new ProtocolException("content body exceeds declared size");
        System.arraycopy(chunk, 0, body, received, chunk.length);
        received += chunk.length;
        return received == body.length;
    }

    public boolean hasPending() {
        return pending != null;
    }

    /** Builds the completed message and resets the assembler. */
    public Object build() {
        Method m = pending;
        ContentHeader h = header;
        byte[] b = body;
        pending = null;
        header = null;
        body = null;
        received = 0;
        if (m instanceof BasicMethods.Deliver) {
            BasicMethods.Deliver d = (BasicMethods.Deliver) m;
            return new Delivery(d.consumerTag(), d.deliveryTag(), d.redelivered(), d.exchange(), d.routingKey(), h.properties(), b);
        } else if (m instanceof BasicMethods.GetOk) {
            BasicMethods.GetOk g = (BasicMethods.GetOk) m;
            return new GetResponse(g.deliveryTag(), g.redelivered(), g.exchange(), g.routingKey(), g.messageCount(), h.properties(), b);
        } else if (m instanceof BasicMethods.Return) {
            BasicMethods.Return r = (BasicMethods.Return) m;
            return new ReturnedMessage(r.replyCode(), r.replyText(), r.exchange(), r.routingKey(), h.properties(), b);
        }
        throw new IllegalStateException("not a content-bearing method: " + m.name());
    }

    public void reset() {
        pending = null;
        header = null;
        body = null;
        received = 0;
    }
}
