# Federation

Federation links brokers together, allowing messages to flow between them. It is designed for loosely-coupled multi-site deployments where full clustering is not appropriate.

## Exchange Federation

Exchange federation replicates bindings from a downstream exchange to an upstream broker, causing matching messages to be forwarded downstream.

How it works:

1. A federation upstream is configured with the remote broker URI
2. A policy with `federation-upstream` or `federation-upstream-set` is applied to the downstream exchange
3. LavinMQ creates a temporary exchange and queue on the upstream broker
4. Bindings on the downstream exchange are mirrored to the upstream
5. Messages matching those bindings are consumed from upstream and published to the downstream exchange

### Hop Limiting

The `max-hops` parameter (default 1) prevents messages from being forwarded in circles in multi-hop federation topologies.

## Queue Federation

Queue federation consumes messages from a queue on an upstream broker and republishes them locally.

How it works:

1. A federation upstream is configured
2. A policy is applied to the local queue
3. LavinMQ connects to the upstream and consumes from the specified queue
4. Messages are published to the local queue

Queue federation is consumer-driven — it only fetches messages when local consumers are present.

## Upstream Configuration

Upstreams are configured as parameters (component: `federation-upstream`).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `uri` | (required) | AMQP URI of the upstream broker |
| `exchange` | (same name) | Upstream exchange name (if different from downstream) |
| `queue` | (same name) | Upstream queue name (if different from downstream) |
| `prefetch-count` | `1000` | Prefetch count for the upstream consumer |
| `reconnect-delay` | `1` | Seconds to wait before reconnecting after failure |
| `ack-mode` | `on-confirm` | When to ack upstream messages: `on-confirm`, `on-publish`, `no-ack` |
| `max-hops` | `1` | Maximum federation hops |
| `expires` | (none) | Upstream queue expiry (milliseconds; forwarded as `x-expires`) |
| `message-ttl` | (none) | Message TTL on the upstream queue (milliseconds; forwarded as `x-message-ttl`) |

## Upstream Sets

An upstream set groups multiple upstreams. Apply it via the `federation-upstream-set` policy key. The special value `all` includes all defined upstreams.

## Policy Activation

Federation is activated by applying policies to exchanges or queues:

| Policy Key | Description |
|-----------|-------------|
| `federation-upstream` | Name of a single upstream to federate with |
| `federation-upstream-set` | Name of an upstream set |

## Reconnection

Federation links automatically reconnect on failure, with a configurable delay between attempts.
