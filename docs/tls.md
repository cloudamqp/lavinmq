# TLS

LavinMQ supports TLS encryption for all protocols: AMQPS, MQTTS, and HTTPS. Each protocol uses a dedicated port that runs the TLS handshake at connect time and then carries the same protocol that the plaintext port would. Certificates, ciphers, and minimum version are shared across all protocols by default; SNI sections (below) can override them per hostname.

## Configuration

TLS is configured globally in the `[main]` section. On startup, LavinMQ builds one TLS context from these options and uses it for every TLS listener; the same certificate, cipher list, and minimum version are shared across AMQPS, MQTTS, and HTTPS unless an SNI section overrides them.

```ini
[main]
tls_cert = /etc/lavinmq/cert.pem
tls_key = /etc/lavinmq/key.pem
```

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `tls_cert` | `[main]` | (empty) | Certificate file (including chain) |
| `tls_key` | `[main]` | (empty) | Private key file. If empty, the cert file is expected to contain both. |
| `tls_ciphers` | `[main]` | (empty) | Allowed cipher list, in OpenSSL cipher list format |
| `tls_min_version` | `[main]` | (empty) | Minimum TLS version. Empty falls back to the TLS library default (1.2). |
| `tls_ktls` | `[main]` | `false` | Enable kernel TLS offloading |
| `tls_keylog_file` | `[main]` | (empty) | Key log file for debugging |

## TLS Ports

Each protocol exposes a dedicated TLS port that runs the TLS handshake on accept and then speaks the same protocol the plaintext port would. The TLS and plaintext ports are independent listeners; either or both can be enabled.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `tls_port` | `[amqp]` | `5671` | AMQPS port |
| `tls_port` | `[mqtt]` | `8883` | MQTTS port |
| `tls_port` | `[mgmt]` | `15671` | HTTPS port |

A TLS port only starts accepting connections when `tls_cert` is configured. If `tls_key` is left empty, the certificate file is expected to contain both the certificate (or chain) and the private key in PEM order.

## mTLS (Mutual TLS)

Mutual TLS extends the TLS handshake so the server requests a certificate from the client and verifies it against a trusted CA bundle. Clients that fail to present a valid certificate are rejected during the handshake, before any AMQP/MQTT/HTTP traffic flows.

Configure the verify flag and CA bundle on the SNI section that should require client certificates:

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `tls_verify_peer` | `[sni:hostname]` | `false` | Require the client to present a certificate during the handshake |
| `tls_ca_cert` | `[sni:hostname]` | (empty) | CA bundle used to validate client certificates |

mTLS only proves that the connecting peer holds a private key trusted by the configured CA. It does not by itself authenticate a LavinMQ user; the client still needs to authenticate via the configured authentication chain (PLAIN credentials, JWT, etc.) once the TLS session is established.

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

During the TLS handshake the client may include a `server_name` extension naming the host it intends to reach. LavinMQ looks the name up in the configured `[sni:hostname]` sections (exact match), and the matching section's certificate, ciphers, minimum version, and mTLS settings are used for that connection. If no SNI section matches — or the client sends no `server_name` at all — the server falls back to the certificate and settings from `[main]`.

## kTLS (Kernel TLS)

When `tls_ktls` is enabled, the TLS handshake still happens in userspace, but once session keys are negotiated they are handed off to the kernel and bulk record encryption/decryption runs there. Application data can then be sent and received through ordinary `read`/`write` (or `sendfile`) syscalls without copying into the userspace TLS library on every record, which reduces context switches and CPU overhead under high throughput. Requires Linux kernel 4.13+ with the `tls` module enabled and a cipher supported by the kernel's kTLS implementation.

## TLS Key Log

Set `tls_keylog_file` to a file path and LavinMQ will append the TLS session secrets for every connection in the [NSS Key Log Format](https://firefox-source-docs.mozilla.org/security/nss/legacy/key_log_format/index.html). Tools like Wireshark can read that file and decrypt captured AMQPS/MQTTS/HTTPS traffic, which is useful when diagnosing handshake or framing issues.

Anyone with access to the key log file can decrypt all traffic to the server for as long as those keys are valid, so this should only be enabled in controlled, non-production environments and the file kept on a tightly permissioned path.
