package com.lavinmq.jms.amqp;

import com.lavinmq.jms.amqp.method.ChannelMethods;
import com.lavinmq.jms.amqp.method.ConnectionMethods;

/** Why a connection or channel was closed. */
public final class ShutdownSignal {
    private final boolean hardError;
    private final boolean initiatedByApplication;
    private final int replyCode;
    private final String replyText;
    private final int classId;
    private final int methodId;
    private final Throwable cause;

    private ShutdownSignal(boolean hardError, boolean initiatedByApplication, int replyCode, String replyText,
                           int classId, int methodId, Throwable cause) {
        this.hardError = hardError;
        this.initiatedByApplication = initiatedByApplication;
        this.replyCode = replyCode;
        this.replyText = replyText;
        this.classId = classId;
        this.methodId = methodId;
        this.cause = cause;
    }

    public static ShutdownSignal fromClose(ConnectionMethods.Close close) {
        return new ShutdownSignal(true, false, close.replyCode(), close.replyText(), close.classId(), close.methodId(), null);
    }

    public static ShutdownSignal fromClose(ChannelMethods.Close close) {
        return new ShutdownSignal(false, false, close.replyCode(), close.replyText(), close.classId(), close.methodId(), null);
    }

    public static ShutdownSignal fromCause(Throwable cause) {
        String text = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        return new ShutdownSignal(true, false, 0, text, 0, 0, cause);
    }

    public static ShutdownSignal application(boolean hardError, int replyCode, String replyText) {
        return new ShutdownSignal(hardError, true, replyCode, replyText, 0, 0, null);
    }

    /** Signal propagated to the channels of a connection that was shut down. */
    public ShutdownSignal forChannel() {
        return new ShutdownSignal(true, initiatedByApplication, replyCode, replyText, classId, methodId, cause);
    }

    /** True when the whole connection is gone, false for a channel-only close. */
    public boolean isHardError() {
        return hardError;
    }

    public boolean isInitiatedByApplication() {
        return initiatedByApplication;
    }

    public int replyCode() {
        return replyCode;
    }

    public String replyText() {
        return replyText;
    }

    public int classId() {
        return classId;
    }

    public int methodId() {
        return methodId;
    }

    public Throwable cause() {
        return cause;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(hardError ? "connection" : "channel");
        sb.append(initiatedByApplication ? " closed by application" : " closed by server");
        if (replyCode != 0) sb.append(": ").append(replyCode).append(' ').append(replyText);
        else if (replyText != null) sb.append(": ").append(replyText);
        if (classId != 0) sb.append(" (class ").append(classId).append(", method ").append(methodId).append(')');
        return sb.toString();
    }
}
