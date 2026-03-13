
LavinMQ supports the following main protocols:

- AMQP
- MQTT
- HTTP
- Replication protocol

Each protocol listens on a default port that's configurable. In the subsequent
sections, we will look at the available ports for each protocol and how to configure
your instance to use custom ports.

## AMQP

In addition to **AMQP**, LavinMQ also supports TLS 1.2 and 1.3, for **AMQPS**.
The ports for AMQP and AMQPS are as follows:

- AMQP: `5672`
- AMQPS: `5671` - If a valid certificate and key are available.

If you are running self-managed LavinMQ, you can also use custom ports for AMQP and AMQPS by
updating the following variables in the `lavinmq.ini` file as shown below:

```
[amqp]
bind = 0.0.0.0
port = 3322
tls_port = 3321
```

## MQTT

In addition to **AMQP**, LavinMQ also supports **MQTT**, a lightweight publish/subscribe protocol commonly used for IoT and resource-constrained devices.

LavinMQ supports both plain **MQTT** and **MQTTS** (MQTT over TLS 1.2 and 1.3).
The default ports for MQTT and MQTTS are as follows:

- MQTT: `1883`
- MQTTS: `8883`

If you are running self-managed LavinMQ, you can also use custom ports for MQTT and MQTTS by
updating the following variables in the `lavinmq.ini` file as shown below:

```
[mqtt]
bind = 0.0.0.0
port = 3342
tls_port = 3341
```

## HTTP

LavinMQ exposes a number of services over HTTP, namely:

- Management UI
- HTTP API
- Prometheus metrics endpoint

In addition to **HTTP**, LavinMQ also supports TLS 1.2 and 1.3, for **HTTPS**.
Thus, for the management interface, LavinMQ listens on the following ports:

- HTTP: `15672`
- HTTPS: `15671` - If a valid certificate and key are available.

Other than the defaults above, you can also use custom ports for HTTP and HTTPS by
updating the following variables in the `lavinmq.ini` file as shown below:

```
[mgmt]
bind = 0.0.0.0
port = 6644
tls_port = 6643
```

In addition to the management interface LavinMQ also exposes a HTTP API and
a metrics endpoint for Prometheus - both services listen on port `443`.

## Replication protocol

LavinMQ `>1.1.6` comes with a dedicated replication protocol for the `hot standby replication feature`. This protocol listens on the default port `5679`.
However, this is configurable.

To use a custom port, update the following variables in the `lavinmq.ini` file as shown below:

```
[replication]
bind = 0.0.0.0
port = 5679
```

## Wrap up

In conclusion, LavinMQ supports three main protocols: AMQP, HTTP,
and Replication protocol. Each protocol has its default port, which can be
configured to use custom ports. By adjusting the configuration settings in
the `lavinmq.ini` file, you can easily tailor LavinMQ to suit your specific
port requirements for each protocol.
