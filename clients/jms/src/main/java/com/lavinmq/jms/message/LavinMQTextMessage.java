package com.lavinmq.jms.message;

import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;

public final class LavinMQTextMessage extends LavinMQMessage implements TextMessage {
    private String text;

    public LavinMQTextMessage() {
    }

    public LavinMQTextMessage(String text) {
        this.text = text;
    }

    @Override
    public void setText(String text) throws JMSException {
        checkBodyWriteable();
        this.text = text;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        text = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (text == null) return null;
        if (c.isAssignableFrom(String.class)) return (T) text;
        throw bodyMismatch(c, "String");
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        return text == null || c.isAssignableFrom(String.class);
    }

    @Override
    public String messageType() {
        return "text";
    }
}
