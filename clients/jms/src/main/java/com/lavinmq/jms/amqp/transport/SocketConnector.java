package com.lavinmq.jms.amqp.transport;

import com.lavinmq.jms.amqp.ConnectionConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/** Opens the TCP (and optionally TLS) socket for a connection. */
public final class SocketConnector {
    private SocketConnector() {
    }

    public static Socket connect(ConnectionConfig cfg) throws IOException {
        Socket socket = new Socket();
        try {
            if (cfg.socketConfigurator() != null) cfg.socketConfigurator().accept(socket);
            socket.connect(new InetSocketAddress(cfg.host(), cfg.port()), cfg.connectionTimeoutMs());
            socket.setSoTimeout(cfg.connectionTimeoutMs());
            if (!cfg.tls()) return socket;
            SSLContext ctx = cfg.sslContext();
            if (ctx == null) {
                try {
                    ctx = SSLContext.getDefault();
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException("no default SSLContext", e);
                }
            }
            SSLSocket ssl = (SSLSocket) ctx.getSocketFactory().createSocket(socket, cfg.host(), cfg.port(), true);
            SSLParameters params = ssl.getSSLParameters();
            params.setEndpointIdentificationAlgorithm("HTTPS");
            if (!isIpLiteral(cfg.host())) params.setServerNames(List.of(new SNIHostName(cfg.host())));
            ssl.setSSLParameters(params);
            ssl.setUseClientMode(true);
            ssl.startHandshake();
            return ssl;
        } catch (IOException | RuntimeException e) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
            throw e;
        }
    }

    private static boolean isIpLiteral(String host) {
        return host.contains(":") || host.matches("\\d+\\.\\d+\\.\\d+\\.\\d+");
    }
}
