package com.lavinmq.jms;

/** Optional, provider-specific notifications about automatic connection recovery. */
public interface RecoveryListener {
    /** The connection was lost and recovery is about to start. */
    default void onRecoveryStarted(Throwable cause) {
    }

    /** The connection, its sessions and consumers have been restored. */
    default void onRecovered() {
    }

    /** Recovery was given up (attempts exhausted) or the connection was closed while recovering. */
    default void onRecoveryFailed(Throwable cause) {
    }
}
