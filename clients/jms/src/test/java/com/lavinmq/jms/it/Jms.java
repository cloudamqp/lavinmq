package com.lavinmq.jms.it;

import com.lavinmq.jms.LavinMQConnectionFactory;
import com.lavinmq.jms.amqp.it.Broker;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Test support for the JMS integration tests. */
public final class Jms {
    private Jms() {
    }

    public static LavinMQConnectionFactory factory() {
        LavinMQConnectionFactory f = new LavinMQConnectionFactory(Broker.URL);
        f.setHeartbeat(10);
        f.setConnectionTimeout(10_000);
        f.setRpcTimeout(10_000);
        f.setConfirmTimeout(10_000);
        f.setRecoveryTimeout(10_000);
        return f;
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static String auth() {
        return "Basic " + Base64.getEncoder().encodeToString((Broker.URI.username() + ":" + Broker.URI.password()).getBytes(StandardCharsets.UTF_8));
    }

    public static String httpGet(String path) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(Broker.httpUrl() + path)).header("Authorization", auth()).GET().build();
        HttpResponse<String> res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 300) throw new IOException("GET " + path + " -> " + res.statusCode() + ": " + res.body());
        return res.body();
    }

    public static int httpDelete(String path) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(Broker.httpUrl() + path)).header("Authorization", auth()).DELETE().build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    public static int httpPut(String path, String json) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(Broker.httpUrl() + path)).header("Authorization", auth())
            .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(json)).build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString()).statusCode();
    }

    public static String vhostPath() {
        return java.net.URLEncoder.encode(Broker.URI.vhost(), StandardCharsets.UTF_8);
    }

    /** Reads an integer field from a flat JSON document (good enough for the management API). */
    public static int jsonInt(String json, String field) {
        Matcher m = Pattern.compile("\"" + field + "\"\\s*:\\s*(-?\\d+)").matcher(json);
        if (!m.find()) throw new IllegalArgumentException("no field " + field + " in " + json);
        return Integer.parseInt(m.group(1));
    }

    public static String queueJson(String queue) throws IOException, InterruptedException {
        return httpGet("/api/queues/" + vhostPath() + "/" + queue);
    }

    /** Polls the HTTP API until the queue reports the expected counts. */
    public static void awaitQueueCounts(String queue, int ready, int unacked) throws Exception {
        String last = "";
        for (int i = 0; i < 100; i++) {
            last = queueJson(queue);
            if (jsonInt(last, "ready") == ready && jsonInt(last, "unacked") == unacked) return;
            Thread.sleep(50);
        }
        throw new AssertionError("queue " + queue + " did not reach ready=" + ready + " unacked=" + unacked + ": " + last);
    }
}
