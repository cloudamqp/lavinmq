package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ShutdownSignal;
import com.lavinmq.jms.destination.LavinMQTemporaryQueue;
import jakarta.jms.JMSException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reconnects after an unexpected connection loss and restores sessions, consumers and temporary queues. */
final class RecoveryManager {
    private static final Logger LOG = System.getLogger(RecoveryManager.class.getName());

    private final LavinMQConnection connection;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Thread thread;
    private volatile boolean cancelled;

    RecoveryManager(LavinMQConnection connection) {
        this.connection = connection;
    }

    boolean isRecoveryThread() {
        return Thread.currentThread() == thread;
    }

    /** Called on the transport's dispatch thread when the connection dies unexpectedly. */
    void onTransportLost(ShutdownSignal signal) {
        Throwable cause = signal.cause() != null ? signal.cause() : new JMSException(signal.toString(), code(signal));
        ConnectionSettings s = connection.settings;
        if (!s.automaticRecovery) {
            LOG.log(Level.WARNING, "connection {0} lost: {1}", connection.id, signal);
            connection.failPermanently(cause);
            return;
        }
        if (!running.compareAndSet(false, true)) return;
        LOG.log(Level.WARNING, "connection {0} lost ({1}); recovering", connection.id, signal);
        connection.gate.recovering(true);
        for (LavinMQSession session : connection.sessions) session.transportLost(cause);
        connection.admin.reset();
        connection.topology.clear();
        JMSException notice = new JMSException("connection lost, recovering: " + signal, "RECOVERING");
        notice.initCause(cause);
        connection.notifyExceptionListener(notice);
        if (s.recoveryListener != null) {
            try {
                s.recoveryListener.onRecoveryStarted(cause);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "RecoveryListener threw", e);
            }
        }
        Thread t = new Thread(this::loop, "lavinmq-jms-recovery-" + connection.id);
        t.setDaemon(true);
        thread = t;
        t.start();
    }

    private static String code(ShutdownSignal s) {
        return s.replyCode() == 0 ? "CONNECTION_LOST" : Integer.toString(s.replyCode());
    }

    private void loop() {
        ConnectionSettings s = connection.settings;
        long delay = Math.max(1, s.recoveryIntervalMs);
        int attempt = 0;
        Throwable last = null;
        while (!cancelled && !connection.isClosed()) {
            attempt++;
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                break;
            }
            if (cancelled || connection.isClosed()) break;
            AmqpConnection t = null;
            try {
                t = connection.connectTransport(connection.nextAddressIndex());
                restore();
                LOG.log(Level.INFO, "connection {0} recovered after {1} attempt(s)", connection.id, attempt);
                running.set(false);
                connection.gate.recovering(false);
                for (LavinMQSession session : connection.sessions) session.wakeAll();
                if (s.recoveryListener != null) {
                    try {
                        s.recoveryListener.onRecovered();
                    } catch (RuntimeException e) {
                        LOG.log(Level.WARNING, "RecoveryListener threw", e);
                    }
                }
                return;
            } catch (Exception e) {
                last = e;
                LOG.log(Level.WARNING, "recovery attempt {0} for connection {1} failed: {2}", attempt, connection.id, e.toString());
                if (t != null) t.abort();
                if (s.recoveryAttempts > 0 && attempt >= s.recoveryAttempts) break;
                delay = Math.min(delay * 2, Math.max(delay, s.recoveryMaxIntervalMs));
            }
        }
        running.set(false);
        if (cancelled || connection.isClosed()) return;
        Throwable cause = last != null ? last : new JMSException("recovery cancelled");
        LOG.log(Level.ERROR, "giving up recovery of connection {0}: {1}", connection.id, cause.toString());
        if (s.recoveryListener != null) {
            try {
                s.recoveryListener.onRecoveryFailed(cause);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "RecoveryListener threw", e);
            }
        }
        connection.failPermanently(cause);
    }

    /** Restores topology, temporary queues, sessions and consumers on the freshly opened transport. */
    private void restore() throws JMSException {
        connection.admin.reset();
        connection.topology.clear();
        for (LavinMQTemporaryQueue q : connection.temporaryQueues()) {
            String oldName = q.name();
            String newName = connection.admin.call("recover temporary queue", ch -> ch.queueDeclare("", false, false, true, false, null).queue());
            q.renameAfterRecovery(newName);
            connection.topology.queues.add(newName);
            LOG.log(Level.DEBUG, "temporary queue {0} recovered as {1}", oldName, newName);
        }
        for (LavinMQSession session : connection.sessions) session.restore();
    }

    void cancel() {
        cancelled = true;
        Thread t = thread;
        if (t != null) t.interrupt();
    }
}
