package com.lavinmq.jms;

import com.lavinmq.jms.amqp.AmqpException;
import com.lavinmq.jms.amqp.AuthenticationFailureException;
import com.lavinmq.jms.amqp.ChannelClosedException;
import com.lavinmq.jms.amqp.ConnectionClosedException;
import com.lavinmq.jms.amqp.RpcTimeoutException;
import jakarta.jms.IllegalStateException;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.InvalidClientIDException;
import jakarta.jms.InvalidClientIDRuntimeException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.InvalidSelectorException;
import jakarta.jms.InvalidSelectorRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.MessageNotWriteableRuntimeException;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.ResourceAllocationRuntimeException;
import jakarta.jms.TransactionInProgressException;
import jakarta.jms.TransactionInProgressRuntimeException;
import jakarta.jms.TransactionRolledBackException;
import jakarta.jms.TransactionRolledBackRuntimeException;
import java.io.IOException;

/** Maps transport errors to JMS exceptions and checked JMS exceptions to their runtime counterparts. */
public final class JMSExceptions {
    private JMSExceptions() {
    }

    /** Converts an exception raised by the AMQP transport into the matching {@link JMSException}. */
    public static JMSException fromAmqp(Throwable t, String context) {
        if (t instanceof JMSException) return (JMSException) t;
        if (t instanceof AuthenticationFailureException) {
            return link(new JMSSecurityException(context + ": " + t.getMessage(), code(((AmqpException) t).replyCode())), t);
        }
        if (t instanceof ChannelClosedException) {
            ChannelClosedException e = (ChannelClosedException) t;
            return link(forReplyCode(e.replyCode(), e.replyText(), context, false), t);
        }
        if (t instanceof ConnectionClosedException) {
            ConnectionClosedException e = (ConnectionClosedException) t;
            if (e.replyCode() == 403) return link(new JMSSecurityException(context + ": " + e.replyText(), "403"), t);
            return link(forReplyCode(e.replyCode(), e.getMessage(), context, true), t);
        }
        if (t instanceof RpcTimeoutException) {
            return link(new JMSException(context + ": " + t.getMessage(), "TIMEOUT"), t);
        }
        if (t instanceof IOException) {
            return link(new JMSException(context + ": " + t.getMessage(), "IO"), t);
        }
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return link(new JMSException(context + ": interrupted", "INTERRUPTED"), t);
        }
        return link(new JMSException(context + ": " + t, "INTERNAL"), t);
    }

    private static JMSException forReplyCode(int code, String text, String context, boolean connection) {
        String msg = context + ": " + (text == null ? "closed" : text);
        String ec = code(code);
        switch (code) {
            case 404:
                return new InvalidDestinationException(msg, ec);
            case 405:
                return new InvalidDestinationException(msg, ec);
            case 403:
                if (text != null && (text.contains("exclusive") || text.contains("active consumer") || text.contains("in use"))) {
                    return new JMSException(msg, ec);
                }
                return new JMSSecurityException(msg, ec);
            case 530:
                return connection ? new JMSSecurityException(msg, ec) : new JMSException(msg, ec);
            case 320:
                return new JMSException(msg, ec);
            default:
                return new JMSException(msg, ec);
        }
    }

    private static String code(int replyCode) {
        return replyCode == 0 ? null : Integer.toString(replyCode);
    }

    private static <T extends JMSException> T link(T e, Throwable cause) {
        if (cause instanceof Exception) e.setLinkedException((Exception) cause);
        e.initCause(cause);
        return e;
    }

    public static JMSException wrap(String message, Throwable cause) {
        return link(new JMSException(message + ": " + cause.getMessage()), cause);
    }

    /** JMS 2.0 simplified API: maps a checked exception to the corresponding runtime exception. */
    public static JMSRuntimeException toRuntime(JMSException e) {
        String msg = e.getMessage();
        String code = e.getErrorCode();
        JMSRuntimeException r;
        if (e instanceof IllegalStateException) r = new IllegalStateRuntimeException(msg, code, e);
        else if (e instanceof InvalidClientIDException) r = new InvalidClientIDRuntimeException(msg, code, e);
        else if (e instanceof InvalidDestinationException) r = new InvalidDestinationRuntimeException(msg, code, e);
        else if (e instanceof InvalidSelectorException) r = new InvalidSelectorRuntimeException(msg, code, e);
        else if (e instanceof JMSSecurityException) r = new JMSSecurityRuntimeException(msg, code, e);
        else if (e instanceof MessageFormatException) r = new MessageFormatRuntimeException(msg, code, e);
        else if (e instanceof MessageNotWriteableException) r = new MessageNotWriteableRuntimeException(msg, code, e);
        else if (e instanceof ResourceAllocationException) r = new ResourceAllocationRuntimeException(msg, code, e);
        else if (e instanceof TransactionInProgressException) r = new TransactionInProgressRuntimeException(msg, code, e);
        else if (e instanceof TransactionRolledBackException) r = new TransactionRolledBackRuntimeException(msg, code, e);
        else r = new JMSRuntimeException(msg, code, e);
        return r;
    }

    public static IllegalStateException illegalState(String message) {
        return new IllegalStateException(message);
    }
}
