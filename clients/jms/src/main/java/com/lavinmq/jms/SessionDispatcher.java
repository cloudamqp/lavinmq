package com.lavinmq.jms;

import com.lavinmq.jms.amqp.Delivery;

/** The single thread that runs a session's message listeners, one message at a time. */
final class SessionDispatcher implements Runnable {
    private final LavinMQSession session;
    final Thread thread;
    private volatile boolean stopped;
    private boolean inFlight;

    SessionDispatcher(LavinMQSession session) {
        this.session = session;
        this.thread = new Thread(this, "lavinmq-jms-session-" + session.connection.id + "-" + session.number);
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    @Override
    public void run() {
        while (!stopped) {
            LavinMQMessageConsumer consumer;
            Delivery delivery;
            synchronized (session.monitor) {
                LavinMQSession.Pick pick = null;
                while (!stopped) {
                    if (session.connection.gate.isStarted() && !session.isFailed()) pick = session.nextListenerDelivery();
                    if (pick != null) break;
                    try {
                        session.monitor.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (stopped || pick == null) return;
                consumer = pick.consumer();
                delivery = pick.delivery();
                inFlight = true;
            }
            try {
                session.dispatch(consumer, delivery);
            } finally {
                synchronized (session.monitor) {
                    inFlight = false;
                    session.monitor.notifyAll();
                }
            }
        }
    }

    boolean isDispatchThread() {
        return Thread.currentThread() == thread;
    }

    /** Blocks until no listener is running (never called from the dispatch thread itself). */
    void awaitIdle() {
        if (isDispatchThread()) return;
        synchronized (session.monitor) {
            while (inFlight) {
                try {
                    session.monitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    void stop() {
        stopped = true;
        synchronized (session.monitor) {
            session.monitor.notifyAll();
        }
        if (!isDispatchThread()) {
            thread.interrupt();
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
