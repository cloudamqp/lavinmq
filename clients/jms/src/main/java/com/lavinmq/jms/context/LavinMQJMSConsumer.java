package com.lavinmq.jms.context;

import com.lavinmq.jms.JMSExceptions;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageListener;

/** Simplified-API wrapper over a {@link MessageConsumer}. */
public final class LavinMQJMSConsumer implements JMSConsumer {
    private final LavinMQJMSContext context;
    private final MessageConsumer consumer;

    LavinMQJMSConsumer(LavinMQJMSContext context, MessageConsumer consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    public MessageConsumer consumer() {
        return consumer;
    }

    @Override
    public String getMessageSelector() {
        return LavinMQJMSContext.unchecked(consumer::getMessageSelector);
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        return LavinMQJMSContext.unchecked(consumer::getMessageListener);
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        LavinMQJMSContext.unchecked(() -> {
            consumer.setMessageListener(listener);
            return null;
        });
    }

    @Override
    public Message receive() {
        return LavinMQJMSContext.unchecked(consumer::receive);
    }

    @Override
    public Message receive(long timeout) {
        return LavinMQJMSContext.unchecked(() -> consumer.receive(timeout));
    }

    @Override
    public Message receiveNoWait() {
        return LavinMQJMSContext.unchecked(consumer::receiveNoWait);
    }

    @Override
    public void close() {
        LavinMQJMSContext.unchecked(() -> {
            consumer.close();
            return null;
        });
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        return body(receive(), c);
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        return body(receive(timeout), c);
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        return body(receiveNoWait(), c);
    }

    private <T> T body(Message m, Class<T> c) {
        if (m == null) return null;
        try {
            return m.getBody(c);
        } catch (JMSException e) {
            throw JMSExceptions.toRuntime(e);
        } catch (ClassCastException e) {
            throw new MessageFormatRuntimeException("message body is not assignable to " + c.getName());
        }
    }

    LavinMQJMSContext context() {
        return context;
    }
}
