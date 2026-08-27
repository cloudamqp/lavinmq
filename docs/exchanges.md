# Exchanges

Exchanges receive published messages and route them to queues based on bindings and routing rules. Each exchange type implements a different routing strategy.

## Common Properties

| Property | Description |
|----------|-------------|
| `name` | Exchange name. The default exchange has an empty name. |
| `type` | Routing algorithm (direct, fanout, topic, headers, x-consistent-hash, x-delayed-message, x-mqtt-topic) |
| `durable` | Survives server restart |
| `auto_delete` | Deleted when the last binding is removed |
| `internal` | Cannot be published to directly by clients |
| `arguments` | Optional arguments (alternate exchange, deduplication, delayed, etc.) |

## Exchange Types

### Direct Exchange

Routes messages to queues where the binding key exactly matches the routing key.

- Pre-declared: `amq.direct`
- One message can be delivered to multiple queues if they share the same binding key

### Fanout Exchange

Routes messages to all bound queues, ignoring the routing key.

- Pre-declared: `amq.fanout`
- Most efficient for broadcast patterns

### Topic Exchange

Routes messages by pattern matching on the routing key. The routing key is a dot-separated string (e.g., `stock.nyse.ibm`).

- Pre-declared: `amq.topic`
- `*` (star) matches exactly one word
- `#` (hash) matches zero or more words

Examples:
- Binding `stock.*.ibm` matches `stock.nyse.ibm` but not `stock.nyse.sub.ibm`
- Binding `stock.#` matches `stock.nyse.ibm`, `stock`, and `stock.nyse`
- Binding `#` matches everything (acts as fanout)

### Headers Exchange

Routes based on message header matching rather than the routing key.

- Pre-declared: `amq.headers`, `amq.match`
- Binding arguments specify the headers to match
- `x-match` argument controls matching behavior:
  - `all` (default) — all specified headers must match
  - `any` — at least one header must match
- Headers starting with `x-` are ignored during matching

### Consistent Hash Exchange

Distributes messages across bound queues using consistent hashing. Each message is routed to exactly one queue.

- Type: `x-consistent-hash`
- The binding key must be a number representing the queue's weight
- Higher weight means a larger share of messages
- `x-hash-on` exchange argument selects what to hash on (default: routing key). Set it to a header name to hash on a header value instead.
- `x-algorithm` exchange argument selects the hash algorithm: `ring` or `jump`. The server-wide default is configurable:

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `default_consistent_hash_algorithm` | `[main]` | `ring` | Default hash algorithm when `x-algorithm` is not set on the exchange |

### MQTT Topic Exchange

Routes messages published by MQTT clients to AMQP queues, matching MQTT topic filters. This is how an AMQP consumer receives MQTT traffic.

- Type: `x-mqtt-topic`
- Not pre-declared — declare one yourself, under any name
- Binding keys are MQTT topic filters, with `/` as the level separator: `+` matches exactly one level, `#` matches zero or more levels and must be last
- Routing keys are MQTT topics verbatim, slashes included. A publish to `a/b/c` reaches queues bound with `a/b/c`, `a/b/#`, `a/+/c`, `#`, and so on. `#` includes the parent level, so `a/b/c/#` matches it too
- Always internal, even if declared with `internal: false`: an AMQP publish carries no MQTT topic, so publishing to it (over AMQP or through the HTTP API) is refused with `ACCESS_REFUSED`
- Delivered messages carry the exchange's own name, the MQTT topic as the routing key, and `delivery_mode` 2
- Exchange arguments and policies are ignored

A message is delivered once per matching binding, so a queue bound with both `sensors/#` and `sensors/+/temp` gets two copies of a publish to `sensors/a/temp`. Bind non-overlapping filters if you want exactly one.

A newly bound queue only receives messages published after the bind. Retained messages are not replayed into it, unlike for an MQTT client subscribing to the same filter. See [MQTT](mqtt.md).

Binding keys are not validated as MQTT topic filters, and an invalid one fails silently instead of being rejected:

| Binding key | Behaviour |
|-------------|-----------|
| `a/#/b` | Treated as `a/#`; everything after `#` is discarded, so it matches **more** than it appears to |
| `sport#`, `a+b` | Wildcards are only recognised as a whole level, so these are literal level names that never match |
| `a.b.#`, `a.b.*` | AMQP topic syntax; accepted as one literal level named `a.b.#` and never matches |

MQTT `SUBSCRIBE` does validate topic filters, so the same string can be rejected for an MQTT client and silently accepted as a binding key here.

### Default Exchange

A nameless direct exchange that every queue is automatically bound to, with the queue's name as the binding key. Publishing to the default exchange with a routing key equal to a queue name delivers directly to that queue.

Cannot be deleted, and cannot be explicitly bound to.

- Pre-declared: `amq.default` (display name only)

## Alternate Exchanges

An alternate exchange receives messages that would otherwise be unroutable (no matching bindings on the primary exchange).

Set via the `x-alternate-exchange` argument (or `alternate-exchange`) when declaring the exchange, or via the `alternate-exchange` policy.

If the alternate exchange also cannot route the message, the message is discarded (or returned to the publisher if the mandatory flag was set on the publish).

## Delayed Exchanges

Any exchange can be made into a delayed exchange. See [Delayed Queues](delayed-queues.md) for details.

Declare the exchange with type `x-delayed-message` and the underlying type as the `x-delayed-type` argument. Alternatively, apply the `delayed-message` policy to an existing exchange. Messages with an `x-delay` header are held in an internal queue and delivered after the delay expires.

## Log Exchange

When enabled, LavinMQ declares an internal topic exchange named `amq.lavinmq.log` and publishes server log records to it. The routing key is the log severity (e.g., `Info`, `Warn`, `Error`) and the body contains the log source and message.

| Config Key | Section | Default | Description |
|-----------|---------|---------|-------------|
| `log_exchange` | `[main]` | `false` | Publish server log records to `amq.lavinmq.log` |

## Exchange-to-Exchange Bindings

Exchanges can be bound to other exchanges. When a message matches a binding, it is forwarded to the destination exchange, which then routes it using its own bindings. This allows building routing topologies.
