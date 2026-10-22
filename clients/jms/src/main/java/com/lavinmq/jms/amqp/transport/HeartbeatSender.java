package com.lavinmq.jms.amqp.transport;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Sends heartbeats when the connection is idle and detects a silent peer. */
public final class HeartbeatSender {
    private final ScheduledExecutorService scheduler;
    private final FrameWriter writer;
    private final LongSupplier lastReceivedNanos;
    private final Runnable onMissedHeartbeat;
    private final long heartbeatNanos;

    public HeartbeatSender(String name, FrameWriter writer, LongSupplier lastReceivedNanos, Runnable onMissedHeartbeat, int heartbeatSeconds) {
        this.writer = writer;
        this.lastReceivedNanos = lastReceivedNanos;
        this.onMissedHeartbeat = onMissedHeartbeat;
        this.heartbeatNanos = TimeUnit.SECONDS.toNanos(heartbeatSeconds);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        long half = Math.max(heartbeatNanos / 2, TimeUnit.MILLISECONDS.toNanos(100));
        scheduler.scheduleAtFixedRate(this::tick, half, half, TimeUnit.NANOSECONDS);
    }

    private void tick() {
        if (System.nanoTime() - lastReceivedNanos.getAsLong() > 2 * heartbeatNanos) {
            onMissedHeartbeat.run();
            return;
        }
        writer.writeHeartbeatIfIdle(heartbeatNanos / 2);
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}
