package com.lavinmq.jms;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Started/stopped/recovering/closed state of a connection, with waits for consumers and producers. */
final class ConnectionGate {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private boolean started;
    private boolean recovering;
    private boolean closed;
    private volatile Throwable failure;

    boolean isStarted() {
        lock.lock();
        try {
            return started && !recovering && !closed;
        } finally {
            lock.unlock();
        }
    }

    boolean isStartedFlag() {
        lock.lock();
        try {
            return started;
        } finally {
            lock.unlock();
        }
    }

    boolean isRecovering() {
        lock.lock();
        try {
            return recovering;
        } finally {
            lock.unlock();
        }
    }

    boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    Throwable failure() {
        return failure;
    }

    void start() {
        lock.lock();
        try {
            started = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void stop() {
        lock.lock();
        try {
            started = false;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void recovering(boolean value) {
        lock.lock();
        try {
            recovering = value;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void close(Throwable cause) {
        lock.lock();
        try {
            closed = true;
            recovering = false;
            if (cause != null && failure == null) failure = cause;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Wakes every waiter so it can re-check the state it is waiting for. */
    void signalAll() {
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits while the connection is recovering, up to the timeout.
     *
     * @return true when the connection is usable, false on timeout or when closed
     */
    boolean awaitUsable(long timeoutMs) throws InterruptedException {
        lock.lock();
        try {
            long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (recovering && !closed) {
                if (nanos <= 0) return false;
                nanos = changed.awaitNanos(nanos);
            }
            return !closed;
        } finally {
            lock.unlock();
        }
    }

    /** Waits until the state changes or the timeout elapses; returns the remaining nanos. */
    long awaitChange(long nanos) throws InterruptedException {
        lock.lock();
        try {
            return nanos <= 0 ? 0 : changed.awaitNanos(nanos);
        } finally {
            lock.unlock();
        }
    }

    void awaitChange() throws InterruptedException {
        lock.lock();
        try {
            changed.await();
        } finally {
            lock.unlock();
        }
    }
}
