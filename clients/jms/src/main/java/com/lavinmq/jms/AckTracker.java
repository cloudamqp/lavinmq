package com.lavinmq.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Delivery tags of a session's channel: buffered (received, not yet handed to the application) and consumed (handed
 * over, not yet acknowledged). Computes the smallest set of ack frames that covers the consumed tags without touching
 * buffered ones.
 */
final class AckTracker {
    /** One basic.ack / basic.nack to send. */
    record Op(long tag, boolean multiple) {
    }

    private final TreeSet<Long> buffered = new TreeSet<>();
    private final TreeSet<Long> consumed = new TreeSet<>();

    synchronized void delivered(long tag) {
        buffered.add(tag);
    }

    /** The application now holds the message. */
    synchronized void consumed(long tag) {
        buffered.remove(tag);
        consumed.add(tag);
    }

    /** The tag was acknowledged (or nacked) individually. */
    synchronized void settled(long tag) {
        buffered.remove(tag);
        consumed.remove(tag);
    }

    synchronized boolean removeBuffered(long tag) {
        return buffered.remove(tag);
    }

    synchronized int consumedCount() {
        return consumed.size();
    }

    synchronized boolean hasConsumed() {
        return !consumed.isEmpty();
    }

    synchronized boolean isEmpty() {
        return buffered.isEmpty() && consumed.isEmpty();
    }

    /** Returns the operations that settle every consumed tag, and forgets them. */
    synchronized List<Op> drainConsumed() {
        List<Op> ops = new ArrayList<>();
        if (consumed.isEmpty()) return ops;
        Long floor = buffered.isEmpty() ? null : buffered.first();
        Long multipleUpTo = floor == null ? consumed.last() : consumed.lower(floor);
        if (multipleUpTo != null) ops.add(new Op(multipleUpTo, true));
        for (Long t : consumed) {
            if (multipleUpTo == null || t > multipleUpTo) ops.add(new Op(t, false));
        }
        consumed.clear();
        return ops;
    }

    /** Returns the buffered tags and forgets them (used when a consumer closes and requeues what it never handed out). */
    synchronized List<Long> drainBuffered(java.util.Collection<Long> tags) {
        List<Long> out = new ArrayList<>();
        for (Long t : tags) if (buffered.remove(t)) out.add(t);
        return out;
    }

    /** Forgets every tag and returns the highest one, or -1 when nothing is outstanding (used by recover()). */
    synchronized long drainAll() {
        long max = -1;
        if (!buffered.isEmpty()) max = Math.max(max, buffered.last());
        if (!consumed.isEmpty()) max = Math.max(max, consumed.last());
        buffered.clear();
        consumed.clear();
        return max;
    }

    synchronized void reset() {
        buffered.clear();
        consumed.clear();
    }
}
