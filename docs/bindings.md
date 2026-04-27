# Bindings

Bindings connect exchanges to queues (or other exchanges), defining the routing rules for messages.

## Queue-to-Exchange Bindings

A binding links a queue to an exchange with a routing key and optional arguments. When a message published to the exchange matches the binding, it is delivered to the queue.

```
exchange --[routing_key, arguments]--> queue
```

The meaning of the routing key depends on the exchange type:

| Exchange Type | Routing Key Role |
|--------------|-----------------|
| Direct | Exact match against message routing key |
| Fanout | Ignored |
| Topic | Pattern match with wildcards (`*`, `#`) |
| Headers | Ignored (matching uses binding arguments) |
| Consistent Hash | Weight (integer) for hash distribution |

## Exchange-to-Exchange Bindings

Exchanges can also be bound to other exchanges. The source exchange routes matching messages to the destination exchange, which then applies its own routing logic. This enables complex routing topologies without application-level forwarding.

## Binding Arguments

Binding arguments provide additional matching criteria beyond the routing key:

- **Headers exchange**: Arguments define the header key-value pairs to match, plus `x-match` (`all` or `any`)
- **Topic exchange**: Arguments are stored but not used for routing (routing uses the key pattern)
- **Other types**: Arguments are stored as metadata

## Multiple Bindings

Multiple bindings can exist between the same exchange and queue with different routing keys or arguments. A message is delivered to the queue once even if it matches multiple bindings to the same queue.

Multiple queues can share the same binding key on a single exchange. The message is delivered to all matching queues.

## Binding Lifecycle

- Bindings are created with `queue.bind` or `exchange.bind`
- Bindings are removed with `queue.unbind` or `exchange.unbind`
- Durable bindings survive server restart (when both the exchange and queue are durable)
- When a queue or exchange is deleted, all its bindings are removed
- If an exchange has `auto_delete` set, it is deleted when its last binding is removed
