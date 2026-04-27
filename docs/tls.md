# TLS

LavinMQ supports TLS encryption for all protocols: AMQPS, MQTTS, and HTTPS.

## Configuration

TLS is configured globally in the `[main]` section. The same certificate and key are used for all protocols.

```ini
[main]
tls_cert = /etc/lavinmq/cert.pem
tls_key = /etc/lavinmq/key.pem
```

| Config Key | CLI Flag | Env Var | Description |
|-----------|----------|---------|-------------|
| `tls_cert` | `--cert` | `LAVINMQ_TLS_CERT_PATH` | Certificate file (including chain) |
| `tls_key` | `--key` | `LAVINMQ_TLS_KEY_PATH` | Private key file |
| `tls_ciphers` | `--ciphers` | `LAVINMQ_TLS_CIPHERS` | Allowed ciphers |
| `tls_min_version` | `--tls-min-version` | `LAVINMQ_TLS_MIN_VERSION` | Minimum TLS version (default 1.2) |
| `tls_ktls` | `--tls-ktls` | — | Enable kernel TLS offloading |
| `tls_keylog_file` | — | — | Key log file for debugging |

## TLS Ports

Each protocol has a dedicated TLS port:

| Protocol | Config Key | Default Port |
|----------|-----------|-------------|
| AMQPS | `tls_port` in `[amqp]` | 5671 |
| MQTTS | `tls_port` in `[mqtt]` | 8883 |
| HTTPS | `tls_port` in `[mgmt]` | 15671 |

TLS ports are active when `tls_cert` is configured. If `tls_key` is not set separately, the certificate file is expected to contain both the certificate and private key.

## mTLS (Mutual TLS)

Mutual TLS requires clients to present a certificate. This can be used for client authentication.

## SNI (Server Name Indication)

Per-hostname TLS certificates can be configured using `[sni:hostname]` sections in the config file:

```ini
[sni:app1.example.com]
tls_cert = /etc/lavinmq/app1.crt
tls_key = /etc/lavinmq/app1.key

[sni:app2.example.com]
tls_cert = /etc/lavinmq/app2.crt
tls_key = /etc/lavinmq/app2.key
```

The server selects the certificate based on the hostname the client requests during the TLS handshake. If no SNI match is found, the default certificate is used.

## kTLS (Kernel TLS)

When `tls_ktls` is enabled, TLS encryption/decryption is offloaded to the kernel, reducing context switches and improving throughput. Requires Linux kernel 4.13+ with kTLS support.

## TLS Key Log

Set `tls_keylog_file` to a file path to log TLS session keys. This is useful for debugging TLS connections with tools like Wireshark. Do not enable in production.
