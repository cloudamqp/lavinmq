package com.lavinmq.jms.destination;

import jakarta.jms.JMSException;
import jakarta.jms.TemporaryTopic;

/** A unique routing key on the topic exchange; subscribers on the owning connection get exclusive queues. */
public final class LavinMQTemporaryTopic extends LavinMQTopic implements TemporaryTopic {
    private static final long serialVersionUID = 1L;

    public interface Owner {
        void deleteTemporaryTopic(LavinMQTemporaryTopic topic) throws JMSException;
    }

    private final String ownerConnectionId;
    private transient Owner owner;

    public LavinMQTemporaryTopic(String name, String ownerConnectionId, Owner owner) {
        super(Kind.TEMP_TOPIC, name);
        this.ownerConnectionId = ownerConnectionId;
        this.owner = owner;
    }

    public LavinMQTemporaryTopic(String name) {
        this(name, DestinationNames.tempTopicOwner(name), null);
    }

    public String ownerConnectionId() {
        return ownerConnectionId;
    }

    public boolean isOwnedBy(String connectionId) {
        return ownerConnectionId != null && ownerConnectionId.equals(connectionId);
    }

    @Override
    public void delete() throws JMSException {
        if (owner == null) throw new JMSException("temporary topic " + name() + " was not created by this connection");
        owner.deleteTemporaryTopic(this);
    }
}
