package com.lavinmq.jms.amqp;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsed {@code amqp://} or {@code amqps://} URI. Several hosts may be given separated by commas
 * ({@code amqp://u:p@host1:5672,host2:5673/vhost}); the first is the primary address.
 */
public record AmqpUri(boolean tls, List<Address> addresses, String username, String password, String vhost,
                      Map<String, String> query) {

    public record Address(String host, int port) {
        @Override
        public String toString() {
            return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
        }
    }

    public static final int DEFAULT_PORT = 5672;
    public static final int DEFAULT_TLS_PORT = 5671;

    public String host() {
        return addresses.get(0).host();
    }

    public int port() {
        return addresses.get(0).port();
    }

    public Integer heartbeatSeconds() {
        return intParam("heartbeat");
    }

    public Integer connectionTimeoutMs() {
        return intParam("connection_timeout");
    }

    public Integer intParam(String key) {
        String v = query.get(key);
        if (v == null) return null;
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("query parameter " + key + " is not a number: " + v);
        }
    }

    public static AmqpUri parse(String s) {
        if (s == null || s.isBlank()) throw new IllegalArgumentException("empty AMQP URI");
        int schemeEnd = s.indexOf("://");
        if (schemeEnd < 0) throw new IllegalArgumentException("not an AMQP URI (missing scheme): " + s);
        String scheme = s.substring(0, schemeEnd).toLowerCase();
        boolean tls;
        if (scheme.equals("amqp")) tls = false;
        else if (scheme.equals("amqps")) tls = true;
        else throw new IllegalArgumentException("unsupported URI scheme '" + scheme + "', expected amqp or amqps");

        String rest = s.substring(schemeEnd + 3);
        String queryPart = null;
        int q = rest.indexOf('?');
        if (q >= 0) {
            queryPart = rest.substring(q + 1);
            rest = rest.substring(0, q);
        }
        String pathPart = null;
        int slash = rest.indexOf('/');
        if (slash >= 0) {
            pathPart = rest.substring(slash + 1);
            rest = rest.substring(0, slash);
        }
        String username = "guest";
        String password = "guest";
        int at = rest.lastIndexOf('@');
        if (at >= 0) {
            String userinfo = rest.substring(0, at);
            rest = rest.substring(at + 1);
            int colon = userinfo.indexOf(':');
            if (colon >= 0) {
                username = decode(userinfo.substring(0, colon));
                password = decode(userinfo.substring(colon + 1));
            } else {
                username = decode(userinfo);
            }
        }
        int defaultPort = tls ? DEFAULT_TLS_PORT : DEFAULT_PORT;
        List<Address> addresses = new ArrayList<>();
        if (rest.isEmpty()) {
            addresses.add(new Address("localhost", defaultPort));
        } else {
            for (String hostPort : rest.split(",")) {
                addresses.add(parseAddress(hostPort.trim(), defaultPort));
            }
        }
        String vhost = (pathPart == null || pathPart.isEmpty()) ? "/" : decode(pathPart);
        Map<String, String> query = new LinkedHashMap<>();
        if (queryPart != null && !queryPart.isEmpty()) {
            for (String kv : queryPart.split("&")) {
                if (kv.isEmpty()) continue;
                int eq = kv.indexOf('=');
                if (eq < 0) query.put(decode(kv), "");
                else query.put(decode(kv.substring(0, eq)), decode(kv.substring(eq + 1)));
            }
        }
        return new AmqpUri(tls, Collections.unmodifiableList(addresses), username, password, vhost, Collections.unmodifiableMap(query));
    }

    static Address parseAddress(String hostPort, int defaultPort) {
        if (hostPort.isEmpty()) throw new IllegalArgumentException("empty host");
        try {
            // Let java.net.URI validate host syntax, including IPv6 literals.
            URI u = new URI("amqp://" + hostPort);
            if (u.getHost() == null) throw new IllegalArgumentException("invalid host: " + hostPort);
            String host = u.getHost();
            if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
            int port = u.getPort() < 0 ? defaultPort : u.getPort();
            return new Address(host, port);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid host: " + hostPort, e);
        }
    }

    private static String decode(String s) {
        return URLDecoder.decode(s.replace("+", "%2B"), StandardCharsets.UTF_8);
    }
}
