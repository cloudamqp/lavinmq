package com.lavinmq.jms.amqp.transport;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Runs tasks one at a time, in submission order, on a shared executor. */
public final class SerialExecutor implements Executor {
    private static final Logger LOG = System.getLogger(SerialExecutor.class.getName());

    private final Executor pool;
    private final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();

    public SerialExecutor(Executor pool) {
        this.pool = pool;
    }

    @Override
    public void execute(Runnable task) {
        queue.add(task);
        schedule();
    }

    private void schedule() {
        if (running.compareAndSet(false, true)) {
            try {
                pool.execute(this::drain);
            } catch (RejectedExecutionException e) {
                running.set(false);
                LOG.log(Level.WARNING, "dispatch pool rejected task; dropping {0} queued callbacks", queue.size());
                queue.clear();
            }
        }
    }

    private void drain() {
        try {
            Runnable r;
            while ((r = queue.poll()) != null) {
                try {
                    r.run();
                } catch (Throwable t) {
                    LOG.log(Level.WARNING, "callback threw", t);
                }
            }
        } finally {
            running.set(false);
            if (!queue.isEmpty()) schedule();
        }
    }
}
