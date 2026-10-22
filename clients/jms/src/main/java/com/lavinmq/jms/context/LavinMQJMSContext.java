package com.lavinmq.jms.context;

import com.lavinmq.jms.JMSExceptions;
import com.lavinmq.jms.LavinMQConnection;
import com.lavinmq.jms.LavinMQSession;
import jakarta.jms.BytesMessage;
import jakarta.jms.ConnectionMetaData;
import jakarta.jms.Destination;
import jakarta.jms.ExceptionListener;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.StreamMessage;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import java.io.Serializable;

/** JMS 2.0 simplified API: one session over a (possibly shared) connection. */
public final class LavinMQJMSContext implements JMSContext {
    private final SharedConnectionRef ref;
    private final LavinMQSession session;
    private final int sessionMode;
    private volatile boolean autoStart = true;
    private volatile boolean closed;

    private LavinMQJMSContext(SharedConnectionRef ref, LavinMQSession session, int sessionMode) {
        this.ref = ref;
        this.session = session;
        this.sessionMode = sessionMode;
    }

    public static LavinMQJMSContext create(LavinMQConnection connection, int sessionMode) {
        try {
            LavinMQSession s = (LavinMQSession) connection.createSession(sessionMode);
            return new LavinMQJMSContext(new SharedConnectionRef(connection), s, sessionMode);
        } catch (JMSException e) {
            try {
                connection.close();
            } catch (JMSException ignored) {
                // report the original error
            }
            throw JMSExceptions.toRuntime(e);
        }
    }

    public LavinMQConnection connection() {
        return ref.connection;
    }

    public LavinMQSession session() {
        return session;
    }

    private void checkOpen() {
        if (closed) throw new IllegalStateRuntimeException("context is closed");
    }

    private void autoStart() {
        if (autoStart) {
            try {
                ref.connection.start();
            } catch (JMSException e) {
                throw JMSExceptions.toRuntime(e);
            }
        }
    }

    interface Call<T> {
        T run() throws JMSException;
    }

    static <T> T unchecked(Call<T> c) {
        try {
            return c.run();
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        }
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        checkOpen();
        LavinMQSession s = unchecked(() -> (LavinMQSession) ref.connection.createSession(sessionMode));
        ref.retain();
        return new LavinMQJMSContext(ref, s, sessionMode);
    }

    @Override
    public JMSProducer createProducer() {
        checkOpen();
        return new LavinMQJMSProducer(this);
    }

    @Override
    public String getClientID() {
        checkOpen();
        return unchecked(ref.connection::getClientID);
    }

    @Override
    public void setClientID(String clientID) {
        checkOpen();
        unchecked(() -> {
            ref.connection.setClientID(clientID);
            return null;
        });
    }

    @Override
    public ConnectionMetaData getMetaData() {
        checkOpen();
        return unchecked(ref.connection::getMetaData);
    }

    @Override
    public ExceptionListener getExceptionListener() {
        checkOpen();
        return unchecked(ref.connection::getExceptionListener);
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) {
        checkOpen();
        unchecked(() -> {
            ref.connection.setExceptionListener(listener);
            return null;
        });
    }

    @Override
    public void start() {
        checkOpen();
        unchecked(() -> {
            ref.connection.start();
            return null;
        });
    }

    @Override
    public void stop() {
        checkOpen();
        unchecked(() -> {
            ref.connection.stop();
            return null;
        });
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        checkOpen();
        this.autoStart = autoStart;
    }

    @Override
    public boolean getAutoStart() {
        return autoStart;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        unchecked(() -> {
            try {
                session.close();
            } finally {
                ref.release();
            }
            return null;
        });
    }

    @Override
    public BytesMessage createBytesMessage() {
        checkOpen();
        return unchecked(session::createBytesMessage);
    }

    @Override
    public MapMessage createMapMessage() {
        checkOpen();
        return unchecked(session::createMapMessage);
    }

    @Override
    public Message createMessage() {
        checkOpen();
        return unchecked(session::createMessage);
    }

    @Override
    public ObjectMessage createObjectMessage() {
        checkOpen();
        return unchecked(session::createObjectMessage);
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) {
        checkOpen();
        return unchecked(() -> session.createObjectMessage(object));
    }

    @Override
    public StreamMessage createStreamMessage() {
        checkOpen();
        return unchecked(session::createStreamMessage);
    }

    @Override
    public TextMessage createTextMessage() {
        checkOpen();
        return unchecked(session::createTextMessage);
    }

    @Override
    public TextMessage createTextMessage(String text) {
        checkOpen();
        return unchecked(() -> session.createTextMessage(text));
    }

    @Override
    public boolean getTransacted() {
        checkOpen();
        return sessionMode == Session.SESSION_TRANSACTED;
    }

    @Override
    public int getSessionMode() {
        checkOpen();
        return sessionMode;
    }

    @Override
    public void commit() {
        checkOpen();
        unchecked(() -> {
            session.commit();
            return null;
        });
    }

    @Override
    public void rollback() {
        checkOpen();
        unchecked(() -> {
            session.rollback();
            return null;
        });
    }

    @Override
    public void recover() {
        checkOpen();
        unchecked(() -> {
            session.recover();
            return null;
        });
    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return createConsumer(destination, null, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        checkOpen();
        JMSConsumer c = new LavinMQJMSConsumer(this, unchecked(() -> session.createConsumer(destination, messageSelector, noLocal)));
        autoStart();
        return c;
    }

    @Override
    public Queue createQueue(String queueName) {
        checkOpen();
        return unchecked(() -> session.createQueue(queueName));
    }

    @Override
    public Topic createTopic(String topicName) {
        checkOpen();
        return unchecked(() -> session.createTopic(topicName));
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name, String messageSelector, boolean noLocal) {
        checkOpen();
        JMSConsumer c = new LavinMQJMSConsumer(this, unchecked(() -> session.createDurableConsumer(topic, name, messageSelector, noLocal)));
        autoStart();
        return c;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name, String messageSelector) {
        checkOpen();
        JMSConsumer c = new LavinMQJMSConsumer(this, unchecked(() -> session.createSharedDurableConsumer(topic, name, messageSelector)));
        autoStart();
        return c;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        return createSharedConsumer(topic, sharedSubscriptionName, null);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName, String messageSelector) {
        checkOpen();
        JMSConsumer c = new LavinMQJMSConsumer(this, unchecked(() -> session.createSharedConsumer(topic, sharedSubscriptionName, messageSelector)));
        autoStart();
        return c;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return createBrowser(queue, null);
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) {
        checkOpen();
        QueueBrowser b = unchecked(() -> session.createBrowser(queue, messageSelector));
        autoStart();
        return b;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        checkOpen();
        return unchecked(session::createTemporaryQueue);
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        checkOpen();
        return unchecked(session::createTemporaryTopic);
    }

    @Override
    public void unsubscribe(String name) {
        checkOpen();
        unchecked(() -> {
            session.unsubscribe(name);
            return null;
        });
    }

    @Override
    public void acknowledge() {
        checkOpen();
        if (sessionMode != Session.CLIENT_ACKNOWLEDGE) return;
        unchecked(() -> {
            session.acknowledgeAllConsumed();
            return null;
        });
    }
}
