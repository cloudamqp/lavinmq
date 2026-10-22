package com.lavinmq.jms.destination;

import jakarta.jms.JMSException;
import jakarta.jms.TemporaryQueue;

/** A server-named exclusive queue that lives as long as the connection that created it. */
public final class LavinMQTemporaryQueue extends LavinMQQueue implements TemporaryQueue {
    private static final long serialVersionUID = 1L;

    public interface Owner {
        void deleteTemporaryQueue(LavinMQTemporaryQueue queue) throws JMSException;
    }

    private final String ownerConnectionId;
    private transient Owner owner;

    public LavinMQTemporaryQueue(String name, String ownerConnectionId, Owner owner) {
        super(Kind.TEMP_QUEUE, name);
        this.ownerConnectionId = ownerConnectionId;
        this.owner = owner;
    }

    /** A temporary queue reconstructed from a JMSReplyTo header; it can be sent to but not consumed from. */
    public LavinMQTemporaryQueue(String name) {
        this(name, null, null);
    }

    public String ownerConnectionId() {
        return ownerConnectionId;
    }

    public boolean isOwnedBy(String connectionId) {
        return ownerConnectionId != null && ownerConnectionId.equals(connectionId);
    }

    /** Called after a recovery: server-named queues get a new name. */
    public void renameAfterRecovery(String newName) {
        rename(newName);
    }

    @Override
    public void delete() throws JMSException {
        if (owner == null) throw new JMSException("temporary queue " + name() + " was not created by this connection");
        owner.deleteTemporaryQueue(this);
    }
}
