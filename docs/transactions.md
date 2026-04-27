# Transactions

AMQP transactions allow batching publishes and acknowledgments into an atomic unit on a single channel.

## Methods

| Method | Description |
|--------|-------------|
| `tx.select` | Enable transaction mode on the channel |
| `tx.commit` | Commit the current transaction |
| `tx.rollback` | Roll back the current transaction |

## What Is Transactional

Within a transaction:

- **Publishes** are buffered and not routed until commit
- **Acks, nacks, and rejects** are deferred until commit

On `tx.commit`:
1. Deferred acks/nacks/rejects are processed first
2. Buffered publishes are routed
3. Data is flushed to disk

On `tx.rollback`:
- Buffered publishes are discarded
- Deferred acks/nacks/rejects are discarded

## Scope

Transactions are scoped to a single channel. Each channel has its own independent transaction state.

## Mutual Exclusivity with Publisher Confirms

Transactions and publisher confirms cannot be used on the same channel. Enabling one after the other results in a channel error.

## Performance Considerations

Transactions add overhead due to buffering and the synchronous commit/rollback cycle. For most use cases, publisher confirms provide better throughput with similar reliability guarantees.
