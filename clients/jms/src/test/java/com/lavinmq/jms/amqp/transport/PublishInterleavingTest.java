package com.lavinmq.jms.amqp.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lavinmq.jms.amqp.AmqpChannel;
import com.lavinmq.jms.amqp.AmqpConnection;
import com.lavinmq.jms.amqp.ConnectionConfig;
import com.lavinmq.jms.amqp.codec.BasicProperties;
import com.lavinmq.jms.amqp.codec.FrameType;
import com.lavinmq.jms.amqp.method.BasicMethods;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class PublishInterleavingTest {
    @Test
    void framesOfConcurrentPublishesNeverInterleave() throws Exception {
        try (FakeAmqpServer server = new FakeAmqpServer()) {
            ConnectionConfig cfg = ConnectionConfig.builder().host("127.0.0.1").port(server.port()).heartbeatSeconds(0).requestedFrameMax(4096).build();
            CompletableFuture<AmqpConnection> f = CompletableFuture.supplyAsync(() -> {
                try {
                    return AmqpConnection.open(cfg);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            FakeAmqpServer.Session s = server.accept();
            s.handshake(2048, 4096, 0);
            AmqpConnection conn = f.get(5, TimeUnit.SECONDS);
            CompletableFuture<AmqpChannel> cf = CompletableFuture.supplyAsync(() -> {
                try {
                    return conn.openChannel();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            s.acceptChannel();
            AmqpChannel ch = cf.get(5, TimeUnit.SECONDS);

            int threads = 8;
            int perThread = 200;
            int bodySize = 3 * 4096;
            List<Thread> workers = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                Thread w = new Thread(() -> {
                    byte[] body = new byte[bodySize];
                    java.util.Arrays.fill(body, (byte) id);
                    for (int i = 0; i < perThread; i++) {
                        try {
                            ch.basicPublish("", "q" + id, false, BasicProperties.EMPTY, body);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                workers.add(w);
                w.start();
            }
            for (Thread w : workers) w.join(20_000);

            int messages = 0;
            for (int m = 0; m < threads * perThread; m++) {
                FakeAmqpServer.Received method = s.poll();
                BasicMethods.Publish pub = method.as(BasicMethods.Publish.class);
                byte expected = (byte) Integer.parseInt(pub.routingKey().substring(1));
                FakeAmqpServer.Received header = s.poll();
                assertEquals(FrameType.HEADER, header.frame().type(), "message " + m);
                int received = 0;
                while (received < bodySize) {
                    FakeAmqpServer.Received body = s.poll();
                    assertEquals(FrameType.BODY, body.frame().type(), "message " + m);
                    assertTrue(body.frame().payload().length <= 4096 - 8);
                    for (byte b : body.frame().payload()) assertEquals(expected, b, "body bytes mixed between publishers");
                    received += body.frame().payload().length;
                }
                assertEquals(bodySize, received);
                messages++;
            }
            assertEquals(threads * perThread, messages);
            conn.abort();
        }
    }
}
