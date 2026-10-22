package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import jakarta.jms.JMSException;
import java.io.IOException;

/**
 * A connection-wide channel for declarations, bindings and deletions. A failed operation (404, 406 ...) closes an AMQP
 * channel, so running these here keeps session channels alive; the admin channel is simply reopened next time.
 */
final class AdminChannel {
    interface Call<T> {
        T apply(AmqpChannel channel) throws IOException;
    }

    private final LavinMQConnection connection;
    private AmqpChannel channel;

    AdminChannel(LavinMQConnection connection) {
        this.connection = connection;
    }

    <T> T call(String context, Call<T> fn) throws JMSException {
        // Wait for a recovery in progress *before* taking the lock: the recovery thread itself needs this channel.
        AmqpConnection amqp = connection.usableTransport();
        synchronized (this) {
            return run(context, fn, amqp);
        }
    }

    private <T> T run(String context, Call<T> fn, AmqpConnection amqp) throws JMSException {
        try {
            if (channel == null || !channel.isOpen() || channel.connection() != amqp) {
                channel = amqp.openChannel();
            }
            return fn.apply(channel);
        } catch (ChannelClosedException e) {
            channel = null;
            throw JMSExceptions.fromAmqp(e, context);
        } catch (ConnectionClosedException e) {
            channel = null;
            throw JMSExceptions.fromAmqp(e, context);
        } catch (IOException e) {
            channel = null;
            throw JMSExceptions.fromAmqp(e, context);
        }
    }

    synchronized void reset() {
        channel = null;
    }

    synchronized void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // connection is going away
            }
            channel = null;
        }
    }
}
