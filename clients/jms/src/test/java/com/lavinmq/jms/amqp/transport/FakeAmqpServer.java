package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameCodec;
import com.lavinmq.jms.amqp.codec.FrameType;
import com.lavinmq.jms.amqp.method.ConnectionMethods;
import com.lavinmq.jms.amqp.method.Method;
import com.lavinmq.jms.amqp.method.MethodDecoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A scripted AMQP server for unit tests: the test drives one accepted connection through {@link Session}. */
public final class FakeAmqpServer implements AutoCloseable {
    public static final int CHANNEL_MAX = 2048;
    public static final int FRAME_MAX = 131072;

    private final ServerSocket server;
    private final List<Socket> sockets = new ArrayList<>();

    public FakeAmqpServer() throws IOException {
        server = new ServerSocket(0);
        server.setSoTimeout(10_000);
    }

    public int port() {
        return server.getLocalPort();
    }

    /** Blocks until a client connects. */
    public Session accept() throws IOException {
        Socket s = server.accept();
        s.setSoTimeout(10_000);
        synchronized (sockets) {
            sockets.add(s);
        }
        return new Session(s);
    }

    @Override
    public void close() throws IOException {
        synchronized (sockets) {
            for (Socket s : sockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // tearing down
                }
            }
        }
        server.close();
    }

    public record Received(int channel, Frame frame, Method method) {
        public <T extends Method> T as(Class<T> type) {
            if (!type.isInstance(method)) throw new AssertionError("expected " + type.getSimpleName() + " but got " + describe());
            return type.cast(method);
        }

        public String describe() {
            return method != null ? method.name() + " on channel " + channel : "frame type " + frame.type() + " on channel " + channel;
        }
    }

    /** One accepted client connection. Reads happen on a background thread into a queue the test polls. */
    public static final class Session implements AutoCloseable {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private final BlockingQueue<Received> inbox = new LinkedBlockingQueue<>();
        private final Thread reader;
        private volatile boolean sawProtocolHeader;
        private volatile Throwable readerError;

        Session(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            byte[] header = new byte[8];
            in.readFully(header);
            sawProtocolHeader = header[0] == 'A' && header[1] == 'M' && header[2] == 'Q' && header[3] == 'P'
                && header[4] == 0 && header[5] == 0 && header[6] == 9 && header[7] == 1;
            reader = new Thread(this::readLoop, "fake-server-reader");
            reader.setDaemon(true);
            reader.start();
        }

        private void readLoop() {
            try {
                while (true) {
                    Frame f = FrameCodec.read(in, 0);
                    Method m = f.type() == FrameType.METHOD ? MethodDecoder.decode(f.payload()) : null;
                    inbox.add(new Received(f.channel(), f, m));
                }
            } catch (IOException e) {
                readerError = e;
            }
        }

        public boolean sawProtocolHeader() {
            return sawProtocolHeader;
        }

        public synchronized void send(Frame f) throws IOException {
            FrameCodec.write(out, f);
            out.flush();
        }

        public void send(int channel, Method m) throws IOException {
            send(Frame.method(channel, m));
        }

        public synchronized void sendContent(int channel, Method m, BasicProperties props, byte[] body) throws IOException {
            FrameCodec.writeContent(out, channel, m, props, body, FRAME_MAX);
            out.flush();
        }

        public synchronized void sendRaw(byte[] bytes) throws IOException {
            out.write(bytes);
            out.flush();
        }

        /** Next frame from the client, or null after 5 seconds. */
        public Received poll() throws InterruptedException {
            return inbox.poll(5, TimeUnit.SECONDS);
        }

        public Received poll(long ms) throws InterruptedException {
            return inbox.poll(ms, TimeUnit.MILLISECONDS);
        }

        /** Next method frame of the given type, skipping heartbeats. */
        public <T extends Method> Received expect(Class<T> type) throws InterruptedException {
            while (true) {
                Received r = poll();
                if (r == null) throw new AssertionError("timed out waiting for " + type.getSimpleName());
                if (r.frame().type() == FrameType.HEARTBEAT) continue;
                r.as(type);
                return r;
            }
        }

        /** Any next non-heartbeat frame. */
        public Received expectAny() throws InterruptedException {
            while (true) {
                Received r = poll();
                if (r == null) throw new AssertionError("timed out waiting for a frame");
                if (r.frame().type() == FrameType.HEARTBEAT) continue;
                return r;
            }
        }

        public boolean clientClosedSocket() {
            return readerError != null;
        }

        public boolean awaitClientClose(long ms) throws InterruptedException {
            long deadline = System.currentTimeMillis() + ms;
            while (readerError == null && System.currentTimeMillis() < deadline) Thread.sleep(10);
            return readerError != null;
        }

        /** Runs the standard handshake with the given tune values and server properties. Returns the start-ok. */
        public ConnectionMethods.StartOk handshake(int channelMax, long frameMax, int heartbeat) throws Exception {
            send(0, new ConnectionMethods.Start(0, 9, Map.of("product", "FakeMQ"), "PLAIN AMQPLAIN", "en_US"));
            ConnectionMethods.StartOk startOk = expect(ConnectionMethods.StartOk.class).as(ConnectionMethods.StartOk.class);
            send(0, new ConnectionMethods.Tune(channelMax, frameMax, heartbeat));
            expect(ConnectionMethods.TuneOk.class);
            expect(ConnectionMethods.Open.class);
            send(0, new ConnectionMethods.OpenOk());
            return startOk;
        }

        public ConnectionMethods.StartOk handshake() throws Exception {
            return handshake(CHANNEL_MAX, FRAME_MAX, 0);
        }

        /** Answers the next channel.open with open-ok and returns the channel id. */
        public int acceptChannel() throws Exception {
            Received r = expect(com.lavinmq.jms.amqp.method.ChannelMethods.Open.class);
            send(r.channel(), new com.lavinmq.jms.amqp.method.ChannelMethods.OpenOk());
            return r.channel();
        }

        /** Answers connection.close with close-ok. */
        public void acceptClose() throws Exception {
            expect(ConnectionMethods.Close.class);
            send(0, new ConnectionMethods.CloseOk());
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
