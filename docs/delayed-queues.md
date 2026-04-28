# Delayed Queues

Delayed messaging allows messages to be held for a specified duration before being delivered to their destination queues.

## How It Works

1. Declare an exchange with type `x-delayed-message` and set `x-delayed-type` to the underlying exchange type (e.g., `direct`, `topic`, `fanout`)
2. Publish messages with the `x-delay` header set to the desired delay in milliseconds
3. The message is held in an internal queue (`amq.delayed-<exchange_name>`)
4. After the delay expires, the message is published to the exchange using the underlying exchange type's routing logic

## Declaration

```
Exchange type: x-delayed-message
Arguments:
  x-delayed-type: direct  (or topic, fanout, headers, etc.)
```

The `x-delayed-type` argument is required and determines how messages are routed after the delay expires.

## Setting the Delay

Set the `x-delay` header on the message to the delay duration in milliseconds:

```
headers:
  x-delay: 5000  (deliver after 5 seconds)
```

Messages without an `x-delay` header are routed immediately (no delay).

Each message's delay is measured independently from when it was published. If you publish one message with `x-delay: 300000` (5 minutes) and another with `x-delay: 600000` (10 minutes) at the same time, the first delivers after 5 minutes and the second after 10 minutes — not 15. Messages do not queue behind each other; the internal store sorts by absolute delivery time, so a later publish with a shorter delay can be delivered before an earlier publish with a longer delay.

## Policy-Based Activation

Any existing exchange can be made into a delayed exchange via the `delayed-message` policy. This adds delayed behavior without redeclaring the exchange.

The `x-delayed-exchange` argument on the exchange also enables delayed mode.

## Internal Queue

Each delayed exchange creates an internal queue named `amq.delayed-<exchange_name>` (or `amq.delayed.<exchange_name>` for queues created before this naming convention; both forms continue to work). This queue:

- Delivers messages in expiration order (messages are stored in arrival order on disk; an in-memory index orders them by delivery time)
- Cannot be consumed from or published to by clients
- Is automatically deleted when the exchange is deleted
- Inherits the durability of the exchange

## Re-delay Prevention

When a delayed message expires and is routed, the `x-delay` header is removed from the message. The server also checks the most recent `x-death` entry to prevent immediate re-entry into the same delayed queue.

## Limitations

- The delayed queue does not support policies (max-length, TTL, etc.)
- Consumer operations (subscribe, ack, reject) are not supported on the internal queue
