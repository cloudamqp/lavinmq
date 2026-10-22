package com.lavinmq.jms;

import com.lavinmq.jms.amqp.ReturnedMessage;
import com.lavinmq.jms.message.LavinMQMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Publisher-confirm bookkeeping of one session channel. */
final class ConfirmTracker {
    static final class Pending {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        final CompletionListener listener;
        final LavinMQMessage message;
        final String messageId;

        Pending(CompletionListener listener, LavinMQMessage message, String messageId) {
            this.listener = listener;
            this.message = message;
            this.messageId = messageId;
        }
    }

    private final ConcurrentSkipListMap<Long, Pending> pending = new ConcurrentSkipListMap<>();
    private final Set<String> returnedIds = new HashSet<>();
    private boolean returnedWithoutId;
    private final Executor callbacks;

    ConfirmTracker(Executor callbacks) {
        this.callbacks = callbacks;
    }

    Pending register(long seq, CompletionListener listener, LavinMQMessage message, String messageId) {
        Pending p = new Pending(listener, message, messageId);
        pending.put(seq, p);
        return p;
    }

    /** A basic.return precedes the confirm of the same message on the dispatch thread. */
    synchronized void onReturn(ReturnedMessage r) {
        String id = r.properties().messageId();
        if (id != null) returnedIds.add(id);
        else returnedWithoutId = true;
    }

    void onConfirm(boolean ack, long tag, boolean multiple) {
        List<Map.Entry<Long, Pending>> done = new ArrayList<>();
        if (multiple) {
            for (Map.Entry<Long, Pending> e : pending.headMap(tag, true).entrySet()) done.add(e);
            pending.headMap(tag, true).clear();
        } else {
            Pending p = pending.remove(tag);
            if (p != null) done.add(Map.entry(tag, p));
        }
        for (Map.Entry<Long, Pending> e : done) {
            Pending p = e.getValue();
            JMSException failure = null;
            boolean returned;
            synchronized (this) {
                returned = (p.messageId != null && returnedIds.remove(p.messageId)) || (p.messageId == null && returnedWithoutId);
                if (p.messageId == null) returnedWithoutId = false;
            }
            if (returned) {
                failure = new InvalidDestinationException("message was returned by the broker: no queue to route to", "312");
            } else if (!ack) {
                failure = new JMSException("message was rejected by the broker (basic.nack)", "NACK");
            }
            complete(p, failure);
        }
    }

    private void complete(Pending p, JMSException failure) {
        if (failure == null) p.future.complete(null);
        else p.future.completeExceptionally(failure);
        if (p.listener != null) {
            JMSException f = failure;
            try {
                callbacks.execute(() -> {
                    if (f == null) p.listener.onCompletion(p.message);
                    else p.listener.onException(p.message, f);
                });
            } catch (java.util.concurrent.RejectedExecutionException ignored) {
                // connection closed
            }
        }
    }

    /** Fails everything outstanding (channel or connection lost). */
    void failAll(JMSException cause) {
        List<Pending> all = new ArrayList<>(pending.values());
        pending.clear();
        for (Pending p : all) complete(p, cause);
    }

    boolean hasPending() {
        return !pending.isEmpty();
    }

    /** Waits until every outstanding confirm has arrived. */
    void awaitAll(long timeoutMs) throws JMSException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!pending.isEmpty()) {
            Map.Entry<Long, Pending> first = pending.firstEntry();
            if (first == null) break;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new JMSException("timed out waiting for publisher confirms", "TIMEOUT");
            try {
                first.getValue().future.get(remaining, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                throw new JMSException("timed out waiting for publisher confirms", "TIMEOUT");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new JMSException("interrupted while waiting for publisher confirms");
            } catch (java.util.concurrent.ExecutionException e) {
                // the failure was reported to that send; keep draining
            }
        }
    }
}
