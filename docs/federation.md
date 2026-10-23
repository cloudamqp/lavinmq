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

Every federated message carries an `x-received-from` header. Each link that forwards the message adds one entry with the URI of the upstream it came from. Before forwarding, the link compares the number of entries with the upstream's `max-hops` and drops a message that already has `max-hops` entries.

`max-hops` defaults to `1`, so a message crosses one link and stops. In a chain A → B → C, messages from A need `max-hops` 2 to reach C.

LavinMQ limits hops but doesn't detect loops. When links form a cycle, such as two brokers that federate from each other or a ring, a message can come back to a broker it already passed through. As long as the message is under `max-hops`, that broker delivers it again. RabbitMQ drops such a message, so a `max-hops` value carried over from RabbitMQ can cause duplicates on LavinMQ.

To deliver each message exactly once in a cycle, set `max-hops` on every link to the number of links a message needs to reach the farthest broker:

| Topology | `max-hops` |
|---|---|
| Two brokers that federate from each other | `1` |
| Ring of N brokers | `N - 1` |

A higher value makes brokers in the cycle receive messages more than once. For example, two brokers with `max-hops` 3 deliver every message twice on both.

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

An upstream set is a named group of upstreams. Configure a set as a parameter (component: `federation-upstream-set`) whose value is a list of entries, each referencing an existing upstream by name and optionally overriding any of the upstream's parameters (`uri`, `prefetch-count`, `reconnect-delay`, `ack-mode`, `exchange`, `queue`, `max-hops`, `expires`, `message-ttl`).

```json
[
  { "upstream": "site-b" },
  { "upstream": "site-c", "max-hops": 2 }
]
```

Apply a set to an exchange or queue via the `federation-upstream-set` policy key. All upstreams in the set are linked.

The special value `all` is reserved: it does not need to be created and dynamically refers to every currently defined upstream.

## Reconnection

Federation links automatically reconnect on failure. The delay between attempts is set per upstream:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reconnect-delay` | `1` | Seconds to wait between reconnection attempts |

Set it on the upstream parameter (or on a set entry) when configuring federation.
