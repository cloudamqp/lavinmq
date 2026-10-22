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

    synchronized <T> T call(String context, Call<T> fn) throws JMSException {
        AmqpConnection amqp = connection.usableTransport();
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
