package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.codec.Frame;
import com.lavinmq.jms.amqp.codec.FrameCodec;
import com.lavinmq.jms.amqp.method.Method;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/** The single write path of a connection. Serializes frames from all threads and flushes per call. */
public final class FrameWriter {
    private final ReentrantLock lock = new ReentrantLock();
    private final DataOutputStream out;
    private final Consumer<Throwable> onError;
    private volatile int frameMax;
    private volatile long lastSentNanos = System.nanoTime();
    private volatile ShutdownSignal closed;

    public FrameWriter(DataOutputStream out, int frameMax, Consumer<Throwable> onError) {
        this.out = out;
        this.frameMax = frameMax;
        this.onError = onError;
    }

    public void frameMax(int frameMax) {
        this.frameMax = frameMax;
    }

    public int frameMax() {
        return frameMax;
    }

    public long lastSentNanos() {
        return lastSentNanos;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void markClosed(ShutdownSignal signal) {
        closed = signal;
    }

    private void checkOpen() throws ConnectionClosedException {
        ShutdownSignal s = closed;
        if (s != null) throw new ConnectionClosedException(s);
    }

    /** Writes raw bytes (the protocol header). Caller must hold the lock. */
    public void writeRaw(byte[] bytes) throws IOException {
        checkOpen();
        try {
            out.write(bytes);
            out.flush();
            lastSentNanos = System.nanoTime();
        } catch (IOException e) {
            fail(e);
            throw e;
        }
    }

    public void write(Frame f) throws IOException {
        lock.lock();
        try {
            checkOpen();
            try {
                FrameCodec.write(out, f);
                out.flush();
                lastSentNanos = System.nanoTime();
            } catch (IOException e) {
                fail(e);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    public void writeAll(Frame... frames) throws IOException {
        lock.lock();
        try {
            checkOpen();
            try {
                for (Frame f : frames) FrameCodec.write(out, f);
                out.flush();
                lastSentNanos = System.nanoTime();
            } catch (IOException e) {
                fail(e);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Writes a content-bearing method with header and body frames atomically. Caller may already hold the lock. */
    public void writeContent(int channel, Method m, BasicProperties props, byte[] body) throws IOException {
        lock.lock();
        try {
            checkOpen();
            try {
                FrameCodec.writeContent(out, channel, m, props, body, frameMax);
                out.flush();
                lastSentNanos = System.nanoTime();
            } catch (IOException e) {
                fail(e);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    /** Sends a heartbeat frame when nothing was written for at least {@code idleNanos}. */
    public void writeHeartbeatIfIdle(long idleNanos) {
        if (System.nanoTime() - lastSentNanos < idleNanos) return;
        if (closed != null) return;
        try {
            write(Frame.heartbeat());
        } catch (IOException ignored) {
            // fail() already notified the connection
        }
    }

    private void fail(IOException e) {
        if (closed == null) {
            closed = ShutdownSignal.fromCause(e);
            onError.accept(e);
        }
    }

    public void close() {
        lock.lock();
        try {
            if (closed == null) closed = ShutdownSignal.application(true, 200, "closed");
            out.close();
        } catch (IOException ignored) {
            // socket is going away
        } finally {
            lock.unlock();
        }
    }
}
