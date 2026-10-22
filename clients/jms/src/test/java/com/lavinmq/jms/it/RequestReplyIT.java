package com.lavinmq.jms.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.jms.Connection;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import jakarta.jms.TemporaryTopic;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.Test;

class RequestReplyIT {
    @Test
    void replyToOfEveryKindRoundTrips() throws Exception {
        try (Connection client = Jms.factory().createConnection(); Connection server = Jms.factory().createConnection()) {
            Session cs = client.createSession();
            Session ss = server.createSession();
            Queue requests = cs.createQueue(Jms.unique("requests"));
            MessageConsumer serverConsumer = ss.createConsumer(ss.createQueue(requests.getQueueName()));
            MessageProducer serverReplier = ss.createProducer(null);
            server.start();
            ss.setMessageListener(null);
            serverConsumer.setMessageListener(m -> {
                try {
                    TextMessage reply = ss.createTextMessage("re: " + ((TextMessage) m).getText());
                    reply.setJMSCorrelationID(m.getJMSMessageID());
                    serverReplier.send(m.getJMSReplyTo(), reply);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            client.start();
            MessageProducer requester = cs.createProducer(requests);

            TemporaryQueue tq = cs.createTemporaryQueue();
            Queue q = cs.createQueue(Jms.unique("replies"));
            Topic t = cs.createTopic(Jms.unique("replies"));
            TemporaryTopic tt = cs.createTemporaryTopic();
            Destination[] replyTos = {tq, q, t, tt};
            for (Destination replyTo : replyTos) {
                MessageConsumer replies = cs.createConsumer(replyTo);
                TextMessage req = cs.createTextMessage("ping " + replyTo);
                req.setJMSReplyTo(replyTo);
                requester.send(req);
                Message reply = replies.receive(5000);
                assertEquals("re: ping " + replyTo, ((TextMessage) reply).getText());
                assertEquals(req.getJMSMessageID(), reply.getJMSCorrelationID());
                assertEquals(replyTo, reply.getJMSDestination());
                replies.close();
            }
            assertTrue(tq.getQueueName().startsWith("amq.gen-"));
        }
    }
}
