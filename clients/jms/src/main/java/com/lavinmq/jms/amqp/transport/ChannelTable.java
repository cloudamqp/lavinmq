package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.AmqpChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/** Registered channels of a connection plus the channel id allocator. */
public final class ChannelTable {
    private final ConcurrentHashMap<Integer, AmqpChannel> channels = new ConcurrentHashMap<>();
    private final BitSet used = new BitSet();
    private final int channelMax;

    public ChannelTable(int channelMax) {
        this.channelMax = channelMax;
    }

    /** Reserves the lowest free id, or returns -1 when every id is in use. */
    public synchronized int allocate() {
        int id = used.nextClearBit(1);
        if (id > channelMax) return -1;
        used.set(id);
        return id;
    }

    public synchronized void free(int id) {
        used.clear(id);
    }

    public void put(AmqpChannel ch) {
        channels.put(ch.id(), ch);
    }

    public AmqpChannel get(int id) {
        return channels.get(id);
    }

    public AmqpChannel remove(int id) {
        return channels.remove(id);
    }

    public Collection<AmqpChannel> all() {
        return new ArrayList<>(channels.values());
    }

    public int size() {
        return channels.size();
    }
}
