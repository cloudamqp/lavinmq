package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.AmqpException;
import com.lavinmq.jms.amqp.AuthenticationFailureException;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.ProtocolException;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameCodec;
import com.lavinmq.jms.amqp.codec.FrameType;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import com.lavinmq.jms.amqp.method.Method;
import com.lavinmq.jms.amqp.method.MethodDecoder;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/** The connection.start … connection.open-ok exchange, run on the opening thread before the reader exists. */
public final class Handshake {
    public static final int MIN_FRAME_MAX = 4096;

    public record Negotiated(int channelMax, int frameMax, int heartbeat, Map<String, Object> serverProperties) {
    }

    private final DataInputStream in;
    private final FrameWriter writer;
    private final ConnectionConfig cfg;

    public Handshake(DataInputStream in, FrameWriter writer, ConnectionConfig cfg) {
        this.in = in;
        this.writer = writer;
        this.cfg = cfg;
    }

    public Negotiated run() throws IOException {
        writer.lock();
        try {
            writer.writeRaw(FrameCodec.PROTOCOL_HEADER);
        } finally {
            writer.unlock();
        }
        ConnectionMethods.Start start = expect(ConnectionMethods.Start.class, false);
        if (!Arrays.asList(start.mechanisms().split(" ")).contains("PLAIN")) {
            throw new AmqpException("server does not offer the PLAIN authentication mechanism (offers: " + start.mechanisms() + ")");
        }
        byte[] response = ("\0" + cfg.username() + "\0" + cfg.password()).getBytes(StandardCharsets.UTF_8);
        writer.write(Frame.method(0, new ConnectionMethods.StartOk(cfg.clientProperties(), "PLAIN", response, "en_US")));
        ConnectionMethods.Tune tune = expect(ConnectionMethods.Tune.class, true);
        int channelMax = negotiateMin(cfg.requestedChannelMax(), tune.channelMax());
        long frameMaxL = negotiateMin(cfg.requestedFrameMax(), tune.frameMax());
        int frameMax = (int) Math.max(MIN_FRAME_MAX, Math.min(frameMaxL, Integer.MAX_VALUE));
        int heartbeat = negotiateHeartbeat(cfg.heartbeatSeconds(), tune.heartbeat());
        writer.write(Frame.method(0, new ConnectionMethods.TuneOk(channelMax, frameMax, heartbeat)));
        writer.frameMax(frameMax);
        writer.write(Frame.method(0, new ConnectionMethods.Open(cfg.vhost())));
        expect(ConnectionMethods.OpenOk.class, false);
        return new Negotiated(channelMax, frameMax, heartbeat, start.serverProperties());
    }

    static int negotiateMin(long client, long server) {
        long v;
        if (client == 0) v = server;
        else if (server == 0) v = client;
        else v = Math.min(client, server);
        return (int) Math.min(v, Integer.MAX_VALUE);
    }

    static int negotiateHeartbeat(int client, int server) {
        if (client == 0 || server == 0) return Math.max(client, server);
        return Math.min(client, server);
    }

    private <T extends Method> T expect(Class<T> type, boolean authenticating) throws IOException {
        Frame f;
        try {
            f = FrameCodec.read(in, 0);
        } catch (EOFException e) {
            if (authenticating) {
                throw new AuthenticationFailureException("connection closed by server during authentication; check credentials");
            }
            throw new ConnectionClosedException(ShutdownSignal.fromCause(e));
        }
        if (f.type() != FrameType.METHOD || f.channel() != 0) {
            throw new ProtocolException("unexpected frame during handshake: type " + f.type() + " on channel " + f.channel());
        }
        Method m = MethodDecoder.decode(f.payload());
        if (type.isInstance(m)) return type.cast(m);
        if (m instanceof ConnectionMethods.Close) {
            ConnectionMethods.Close close = (ConnectionMethods.Close) m;
            try {
                writer.write(Frame.method(0, new ConnectionMethods.CloseOk()));
            } catch (IOException ignored) {
                // the server is closing anyway
            }
            if (close.replyCode() == 403) throw new AuthenticationFailureException(403, close.replyText());
            throw new ConnectionClosedException(ShutdownSignal.fromClose(close));
        }
        throw new ProtocolException("expected " + type.getSimpleName() + " during handshake, got " + m.name());
    }
}
