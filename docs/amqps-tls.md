
LavinMQ supports TLS 1.2 and 1.3, both for AMQPS and HTTPS.

If a valid certificate and key are available, LavinMQ will listen on port `5671`
for AMQPS and port `15671` for HTTPS.

The location of the valid certificate and key could be configured in the
`lavinmq.ini` file as shown below:

### Default config

```ini
[main]
tls_cert = /etc/lavinmq/cert.pem
tls_key = /etc/lavinmq/key.pem
tls_min_version = 1.2

[mgmt]
tls_port = 15671

[amqp]
tls_port = 5671
```

### Reloading certificates

Send a `HUP` signal to reload certificate if files have changed e.g. renewed.
Existing connections will not be interrupted, and new TLS connections will be
served the new certificate.

## Wrap up

You can configure TLS in LavinMQ to ensure a secure and robust
communication environment. LavinMQ supports both TLS 1.2 and 1.3, for AMQPS and HTTPS.
LavinMQ requires valid certificate and key for TLS to work. You can configure the location
of these certificates and keys in the `lavinmq.ini` file.
