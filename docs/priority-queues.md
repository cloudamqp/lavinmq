# Priority Queues

Priority queues deliver messages in priority order rather than FIFO order. Higher-priority messages are delivered before lower-priority ones.

## Declaration

Declare a priority queue by setting the `x-max-priority` argument:

```
x-max-priority: 10
```

The value defines the maximum priority level (0-255). Messages with a priority higher than this value are accepted, but clamped to the maximum.

The `x-max-priority` argument is required at declaration time and cannot be changed after creation.

## Priorities Above the Maximum

The queue does not reject a message with a priority above `x-max-priority`. Instead, the queue clamps the priority to `x-max-priority`. Clamped messages share the same effective priority, so the queue keeps no order between their original priorities.

Example with `x-max-priority: 5`:

1. A publisher sends three messages with priorities 6, 7, and 8
2. The queue stores all three messages with the effective priority 5
3. The queue delivers them in publish order, not in the order 8, 7, 6

## How It Works

Internally, a priority queue maintains separate message stores for each priority level (0 through `x-max-priority`). When delivering messages:

1. The store with the highest priority is checked first
2. If it has messages, the next message is delivered from that store
3. Otherwise, the next lower priority store is checked, and so on

Messages without a `priority` property default to priority 0 (lowest).

## Performance Considerations

Each priority level creates its own message store subdirectory on disk (`prio.000`, `prio.001`, etc.). Setting a very high `x-max-priority` increases resource usage proportionally. Choose the minimum number of levels needed for your use case.

## Interaction with Other Features

Priority queues support all standard queue features:

- TTL (message and queue expiration)
- Dead-lettering
- Max length / max length bytes (overflow drops the oldest message from the highest-priority non-empty sub-store)
- Durability
- Consumer prefetch
- Single active consumer
