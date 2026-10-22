package com.lavinmq.jms.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** A TCP relay in front of the broker that tests can cut or block to simulate network failures. */
public final class TcpProxy implements AutoCloseable {
    private final ServerSocket server;
    private final String targetHost;
    private final int targetPort;
    private final List<Socket> sockets = new ArrayList<>();
    private final AtomicInteger accepted = new AtomicInteger();
    private volatile boolean refuse;
    private volatile boolean closed;

    public TcpProxy(String targetHost, int targetPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.server = new ServerSocket();
        this.server.bind(new InetSocketAddress("127.0.0.1", 0));
        Thread t = new Thread(this::acceptLoop, "tcp-proxy-" + server.getLocalPort());
        t.setDaemon(true);
        t.start();
    }

    public int port() {
        return server.getLocalPort();
    }

    public int accepted() {
        return accepted.get();
    }

    /** Refuse (immediately close) new connections until {@link #resume()}. */
    public void refuse() {
        refuse = true;
    }

    public void resume() {
        refuse = false;
    }

    /** Close every relayed connection; the client sees EOF. */
    public synchronized void dropAll() {
        for (Socket s : sockets) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing
            }
        }
        sockets.clear();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                if (refuse) {
                    client.close();
                    continue;
                }
                Socket upstream = new Socket(targetHost, targetPort);
                client.setTcpNoDelay(true);
                upstream.setTcpNoDelay(true);
                synchronized (this) {
                    sockets.add(client);
                    sockets.add(upstream);
                }
                accepted.incrementAndGet();
                pump(client, upstream);
                pump(upstream, client);
            } catch (IOException e) {
                if (closed) return;
            }
        }
    }

    private static void pump(Socket from, Socket to) {
        Thread t = new Thread(() -> {
            byte[] buf = new byte[64 * 1024];
            try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                int n;
                while ((n = in.read(buf)) >= 0) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // one side went away
            } finally {
                try {
                    from.close();
                } catch (IOException ignored) {
                    // closing
                }
                try {
                    to.close();
                } catch (IOException ignored) {
                    // closing
                }
            }
        });
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        dropAll();
        server.close();
    }
}
