package com.lavinmq.jms.amqp;

/** The server refused the credentials (or closed the socket during authentication). */
public class AuthenticationFailureException extends AmqpException {
    private static final long serialVersionUID = 1L;
    private final int replyCode;
    private final String replyText;

    public AuthenticationFailureException(String message) {
        this(403, message);
    }

    public AuthenticationFailureException(int replyCode, String replyText) {
        super(replyText);
        this.replyCode = replyCode;
        this.replyText = replyText;
    }

    @Override
    public int replyCode() {
        return replyCode;
    }

    @Override
    public String replyText() {
        return replyText;
    }
}
