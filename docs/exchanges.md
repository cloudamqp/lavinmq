# Exchanges

Exchanges receive published messages and route them to queues based on bindings and routing rules. Each exchange type implements a different routing strategy.

## Common Properties

| Property | Description |
|----------|-------------|
| `name` | Exchange name. The default exchange has an empty name. |
| `type` | Routing algorithm (direct, fanout, topic, headers, x-consistent-hash, x-delayed-message) |
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

- Pre-declared: `amq.headers`
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
- `x-algorithm` exchange argument selects the hash algorithm: `ring` (default) or `jump`
- The default algorithm can be set globally via the `default_consistent_hash_algorithm` config option

### Default Exchange

A nameless direct exchange that every queue is automatically bound to, with the queue's name as the binding key. Publishing to the default exchange with a routing key equal to a queue name delivers directly to that queue.

Cannot be deleted, and cannot be explicitly bound to.

## Alternate Exchanges

An alternate exchange receives messages that would otherwise be unroutable (no matching bindings on the primary exchange).

Set via the `x-alternate-exchange` argument (or `alternate-exchange`) when declaring the exchange, or via the `alternate-exchange` policy.

If the alternate exchange also cannot route the message, the message is discarded (or returned to the publisher if the mandatory flag was set on the original exchange).

## Delayed Exchanges

Any exchange can be made into a delayed exchange. See [Delayed Queues](delayed-queues.md) for details.

When an exchange has delayed mode enabled (via `x-delayed-exchange` argument or `delayed-message` policy), messages with an `x-delay` header are held in an internal queue and delivered after the delay expires.

## Event Exchange

LavinMQ declares an internal `amq.topic` exchange that can emit system events (queue created, connection closed, etc.) when enabled via the `log_exchange` config option.

## Exchange-to-Exchange Bindings

Exchanges can be bound to other exchanges. When a message matches a binding, it is forwarded to the destination exchange, which then routes it using its own bindings. This allows building routing topologies.

## Deduplication

Exchanges support message deduplication via the `x-message-deduplication` argument. See [Deduplication](deduplication.md) for details.

## Federation

Exchanges can be federated to receive messages from upstream brokers. See [Federation](federation.md) for details.
