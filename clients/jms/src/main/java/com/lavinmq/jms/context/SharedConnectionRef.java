package com.lavinmq.jms.context;

import com.lavinmq.jms.LavinMQConnection;
import jakarta.jms.JMSException;
import java.util.concurrent.atomic.AtomicInteger;

/** Reference count of the contexts sharing one connection; the connection closes with the last context. */
final class SharedConnectionRef {
    final LavinMQConnection connection;
    private final AtomicInteger count = new AtomicInteger(1);

    SharedConnectionRef(LavinMQConnection connection) {
        this.connection = connection;
    }

    void retain() {
        count.incrementAndGet();
    }

    void release() throws JMSException {
        if (count.decrementAndGet() == 0) connection.close();
    }
}
