# Federation

Federation links brokers together, allowing messages to flow between them. It is designed for loosely-coupled multi-site deployments where full clustering is not appropriate.

## Upstreams in this broker

An upstream URI without host, `amqp://` (the `/` vhost) or `amqp:///vhost`, is a vhost of this broker. The link then runs in-process: it consumes and declares directly in that vhost, without an AMQP connection and without logging in as any user. This federates between vhosts of the same broker. Since the link has no credentials of its own, the user creating the upstream (or an upstream set entry with such a `uri`) must have permissions in that vhost, including `read` and `configure` on the upstream `exchange` and `queue` if they are set. Any URI with a host, `localhost` included, connects over AMQP with the URI's credentials.

On the downstream side, messages are always published in-process.

## Exchange Federation

Exchange federation replicates bindings from a downstream exchange to an upstream broker, causing matching messages to be forwarded downstream.

How it works:

1. A federation upstream is configured with the remote broker URI
2. A policy with `federation-upstream` or `federation-upstream-set` is applied to the downstream exchange
3. LavinMQ creates a temporary exchange and queue on the upstream broker
4. Bindings on the downstream exchange are mirrored to the upstream
5. Messages matching those bindings are consumed from upstream and published to the downstream exchange

### Hop Limiting

In multi-hop federation topologies (A → B → C → ...), the same message could otherwise loop indefinitely. To prevent this, every federated message carries an `x-received-from` header that records each broker it has been forwarded through. Before forwarding, the link compares the size of that list to the upstream's `max-hops` parameter — if the list is already at or above `max-hops`, the message is dropped instead of being re-federated.

`max-hops` defaults to `1`, meaning a message is federated once and then stops. Increase it to allow chains across more brokers; the value is the maximum number of hops a single message may take.

## Queue Federation

Queue federation consumes messages from a queue on an upstream broker and republishes them locally.

How it works:

1. A federation upstream is configured
2. A policy is applied to the local queue
3. LavinMQ connects to the upstream and consumes from the specified queue
4. Messages are published to the local queue

Queue federation is consumer-driven: it only consumes from the upstream while the local queue has consumers, and only moves a message when a local consumer is ready to take it. Messages are published to the local queue as `immediate`; when no consumer has room, the message goes back to the upstream queue and the link waits until one has. When the last local consumer leaves, the link closes its upstream session, returning the messages it holds, so they stay available upstream.

## Upstream Configuration

Upstreams are configured as parameters (component: `federation-upstream`).

| Parameter | Default | Description |
|-----------|---------|-------------|
| `uri` | (required) | AMQP URI of the upstream, see [Upstreams in this broker](#upstreams-in-this-broker) |
| `exchange` | (same name) | Upstream exchange name (if different from downstream) |
| `queue` | (same name) | Upstream queue name (if different from downstream) |
| `prefetch-count` | `1000` | Prefetch count for the upstream consumer |
| `reconnect-delay` | `1` | Seconds to wait before reconnecting after failure |
| `ack-mode` | `on-confirm` | When to ack upstream messages: `on-confirm` once the local publish is confirmed (durable), `on-publish` right after publishing locally, or `no-ack` |
| `consumer-tag` | `federation-link-<name>` | Consumer tag of the link's upstream consumer |
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
