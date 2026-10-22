package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameCodec;
import com.lavinmq.jms.amqp.codec.FrameType;
import com.lavinmq.jms.amqp.method.ChannelMethods;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import com.lavinmq.jms.amqp.method.Method;
import com.lavinmq.jms.amqp.method.MethodDecoder;
import com.lavinmq.jms.amqp.transport.ChannelTable;
import com.lavinmq.jms.amqp.transport.FrameWriter;
import com.lavinmq.jms.amqp.transport.Handshake;
import com.lavinmq.jms.amqp.transport.HeartbeatSender;
import com.lavinmq.jms.amqp.transport.SerialExecutor;
import com.lavinmq.jms.amqp.transport.SocketConnector;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** One AMQP 0-9-1 connection: socket, reader thread, heartbeats and the channels multiplexed over it. */
public final class AmqpConnection {
    private static final Logger LOG = System.getLogger(AmqpConnection.class.getName());
    private static final AtomicInteger POOL_SEQ = new AtomicInteger();

    private final ConnectionConfig config;
    private final Socket socket;
    private final DataInputStream in;
    private final FrameWriter writer;
    private final ChannelTable channels;
    private final ExecutorService dispatchPool;
    private final SerialExecutor dispatcher;
    private final HeartbeatSender heartbeat;
    private final Thread reader;
    private final int channelMax;
    private final int frameMax;
    private final int heartbeatSeconds;
    private final Map<String, Object> serverProperties;
    private final List<ConnectionListener> listeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<ShutdownSignal> shutdownCause = new AtomicReference<>();
    private final CompletableFuture<Void> closeOkFuture = new CompletableFuture<>();
    private volatile long lastReceivedNanos = System.nanoTime();

    private AmqpConnection(ConnectionConfig config, Socket socket, DataInputStream in, FrameWriter writer, Handshake.Negotiated n) {
        this.config = config;
        this.socket = socket;
        this.in = in;
        this.writer = writer;
        this.channelMax = n.channelMax();
        this.frameMax = n.frameMax();
        this.heartbeatSeconds = n.heartbeat();
        this.serverProperties = Collections.unmodifiableMap(n.serverProperties());
        this.channels = new ChannelTable(channelMax);
        String name = config.host() + ":" + config.port();
        int poolId = POOL_SEQ.incrementAndGet();
        ThreadFactory tf = config.threadFactory() != null ? config.threadFactory() : daemonFactory("lavinmq-amqp-dispatch-" + poolId + "-");
        this.dispatchPool = Executors.newCachedThreadPool(tf);
        this.dispatcher = new SerialExecutor(dispatchPool);
        this.heartbeat = heartbeatSeconds > 0
            ? new HeartbeatSender("lavinmq-amqp-heartbeat-" + name, writer, () -> lastReceivedNanos,
                () -> shutdown(ShutdownSignal.fromCause(new SocketTimeoutException("missed heartbeats from server"))), heartbeatSeconds)
            : null;
        this.reader = new Thread(this::readLoop, "lavinmq-amqp-reader-" + name);
        this.reader.setDaemon(true);
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger seq = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** Connects, performs the handshake and starts the reader thread. Blocks until connection.open-ok. */
    public static AmqpConnection open(ConnectionConfig config) throws IOException {
        Socket socket = SocketConnector.connect(config);
        FrameWriter writer = null;
        try {
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),
                Math.max(config.requestedFrameMax(), 64 * 1024)));
            AtomicReference<AmqpConnection> ref = new AtomicReference<>();
            writer = new FrameWriter(out, config.requestedFrameMax(), e -> {
                AmqpConnection c = ref.get();
                if (c != null) c.shutdown(ShutdownSignal.fromCause(e));
            });
            Handshake.Negotiated n = new Handshake(in, writer, config).run();
            AmqpConnection conn = new AmqpConnection(config, socket, in, writer, n);
            ref.set(conn);
            socket.setSoTimeout(n.heartbeat() > 0 ? n.heartbeat() * 2 * 1000 : 0);
            conn.reader.start();
            if (conn.heartbeat != null) conn.heartbeat.start();
            return conn;
        } catch (IOException | RuntimeException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
            throw e;
        }
    }

    public ConnectionConfig config() {
        return config;
    }

    public int frameMax() {
        return frameMax;
    }

    public int channelMax() {
        return channelMax;
    }

    public int heartbeatSeconds() {
        return heartbeatSeconds;
    }

    public Map<String, Object> serverProperties() {
        return serverProperties;
    }

    public boolean isOpen() {
        return shutdownCause.get() == null;
    }

    public ShutdownSignal shutdownCause() {
        return shutdownCause.get();
    }

    public void addListener(ConnectionListener l) {
        listeners.add(l);
    }

    public void removeListener(ConnectionListener l) {
        listeners.remove(l);
    }

    FrameWriter writer() {
        return writer;
    }

    ExecutorService dispatchPool() {
        return dispatchPool;
    }

    ChannelTable channels() {
        return channels;
    }

    /** Opens a new channel (channel.open RPC). */
    public AmqpChannel openChannel() throws IOException {
        ShutdownSignal s = shutdownCause.get();
        if (s != null) throw new ConnectionClosedException(s);
        int id = channels.allocate();
        if (id < 0) throw new AmqpException("no free channel ids (channel max " + channelMax + ")");
        AmqpChannel ch = new AmqpChannel(this, id);
        channels.put(ch);
        try {
            ch.open();
            return ch;
        } catch (IOException | RuntimeException e) {
            channels.remove(id);
            channels.free(id);
            throw e;
        }
    }

    /** Graceful close: connection.close / close-ok handshake, then teardown. */
    public void close() throws IOException {
        close(200, "Goodbye", config.rpcTimeoutMs());
    }

    public void close(int replyCode, String replyText, long timeoutMs) throws IOException {
        if (shutdownCause.get() != null) return;
        ShutdownSignal signal = ShutdownSignal.application(true, replyCode, replyText);
        try {
            writer.write(Frame.method(0, new ConnectionMethods.Close(replyCode, replyText, 0, 0)));
            closeOkFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (IOException | TimeoutException | ExecutionException e) {
            LOG.log(Level.DEBUG, "graceful close did not complete: {0}", e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown(signal);
        }
    }

    /** Immediate teardown without the close handshake. */
    public void abort() {
        shutdown(ShutdownSignal.application(true, 200, "aborted"));
    }

    void shutdown(ShutdownSignal cause) {
        if (!shutdownCause.compareAndSet(null, cause)) return;
        writer.markClosed(cause);
        if (heartbeat != null) heartbeat.stop();
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing to do
        }
        ShutdownSignal chSignal = cause.forChannel();
        for (AmqpChannel ch : channels.all()) {
            channels.remove(ch.id());
            ch.processShutdown(chSignal);
        }
        dispatcher.execute(() -> {
            for (ConnectionListener l : listeners) {
                try {
                    l.onShutdown(cause);
                } catch (RuntimeException e) {
                    LOG.log(Level.WARNING, "connection listener threw", e);
                }
            }
        });
        closeOkFuture.complete(null);
    }

    void channelClosed(int id) {
        channels.remove(id);
        channels.free(id);
    }

    private void readLoop() {
        try {
            while (true) {
                Frame f = FrameCodec.read(in, frameMax);
                lastReceivedNanos = System.nanoTime();
                if (f.type() == FrameType.HEARTBEAT) {
                    if (f.channel() != 0) throw new ProtocolException("heartbeat on channel " + f.channel());
                    continue;
                }
                if (f.channel() == 0) {
                    handleConnectionFrame(f);
                    continue;
                }
                AmqpChannel ch = channels.get(f.channel());
                if (ch == null) continue; // frames trailing a close
                ch.handleFrame(f);
            }
        } catch (ProtocolException e) {
            LOG.log(Level.WARNING, "protocol error, closing connection: {0}", e.getMessage());
            try {
                writer.write(Frame.method(0, new ConnectionMethods.Close(501, truncate(e.getMessage()), 0, 0)));
            } catch (IOException ignored) {
                // going down anyway
            }
            shutdown(ShutdownSignal.fromCause(e));
        } catch (SocketTimeoutException e) {
            shutdown(ShutdownSignal.fromCause(new SocketTimeoutException("missed heartbeats from server")));
        } catch (EOFException e) {
            if (shutdownCause.get() == null) LOG.log(Level.DEBUG, "connection closed by server");
            shutdown(ShutdownSignal.fromCause(e));
        } catch (IOException e) {
            if (shutdownCause.get() == null) LOG.log(Level.DEBUG, "connection error: {0}", e.toString());
            shutdown(ShutdownSignal.fromCause(e));
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "reader loop failed", e);
            shutdown(ShutdownSignal.fromCause(e));
        }
    }

    private static String truncate(String s) {
        if (s == null) return "";
        return s.length() > 200 ? s.substring(0, 200) : s;
    }

    private void handleConnectionFrame(Frame f) throws IOException {
        if (f.type() != FrameType.METHOD) throw new ProtocolException("content frame on channel 0");
        Method m = MethodDecoder.decode(f.payload());
        if (m instanceof ConnectionMethods.Close) {
            ConnectionMethods.Close close = (ConnectionMethods.Close) m;
            try {
                writer.write(Frame.method(0, new ConnectionMethods.CloseOk()));
            } catch (IOException ignored) {
                // socket may already be gone
            }
            shutdown(ShutdownSignal.fromClose(close));
        } else if (m instanceof ConnectionMethods.CloseOk) {
            closeOkFuture.complete(null);
        } else if (m instanceof ConnectionMethods.Blocked) {
            String reason = ((ConnectionMethods.Blocked) m).reason();
            dispatcher.execute(() -> listeners.forEach(l -> l.onBlocked(reason)));
        } else if (m instanceof ConnectionMethods.Unblocked) {
            dispatcher.execute(() -> listeners.forEach(ConnectionListener::onUnblocked));
        } else if (m instanceof ChannelMethods.CloseOk || m instanceof ChannelMethods.Close) {
            throw new ProtocolException("channel method " + m.name() + " on channel 0");
        } else {
            throw new ProtocolException("unexpected method on channel 0: " + m.name());
        }
    }

    @Override
    public String toString() {
        return "AmqpConnection[" + config.host() + ":" + config.port() + "/" + config.vhost() + (isOpen() ? "" : ", closed") + "]";
    }
}
