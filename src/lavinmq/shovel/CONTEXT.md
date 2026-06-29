# Shovel

Moves messages from a **Source** to a **Destination**, settling each message on
the source according to what the destination reports. Lives in
`src/lavinmq/shovel/`.

## Language

**Shovel**:
A configured, long-running message mover from one Source to one Destination,
owned by a vhost and driven by a Runner.
_Avoid_: pump, bridge, forwarder.

**Source**:
Where a Shovel reads messages from and settles them (ack / reject). Today only
AMQP queues. The Source owns consume and settlement; it never decides *whether*
a message succeeded.
_Avoid_: origin, input, upstream.

**Destination**:
Where a Shovel delivers messages (AMQP exchange/queue or HTTP endpoint). A
Destination delivers a message and reports an **Outcome**; it never touches the
Source.
_Avoid_: sink, target, output, downstream.

**Runner**:
The single fiber that owns a Shovel's run loop and its **policy**: it maps each
**Outcome** to a Source action and owns retry timing, backoff, and the abort
threshold. The Source classifies nothing; the Destination times nothing.
_Avoid_: worker, driver, supervisor.

**MultiDestinationHandler**:
The failover Destination wrapping a Shovel's list of `dest-uri`s. Holds one
*active* destination at a time; on an **Abort** outcome or a connection failure
it advances to the next, and emits Abort upward only once every destination has
failed with no intervening **Confirmed**. Name kept for continuity — it is a
failover handler, not fan-out or load-balancing.
_Avoid_: RandomDestination, load-balancer, fan-out, round-robin.

**Outcome**:
The per-message disposition a Destination reports back to the Runner. The
Destination maps its native result (HTTP status, AMQP confirm) to one of these;
the Runner decides what each one does. One of:

- **Confirmed** — delivered. Runner acks the message and resets failure counters.
- **Retry** — transient failure (HTTP 5xx/429/408/timeout/connection-refused;
  AMQP nack such as reject-publish overflow). Runner requeues (`requeue: true`)
  and retries with backoff, unbounded.
- **Reject** — the *message* is unacceptable (HTTP 400/422). Runner rejects
  without requeue (`requeue: false`) so the source queue's dead-letter exchange
  handles it, then continues with the next message.
- **Abort** — the *destination* is unusable (HTTP 404, auth failure). Runner
  keeps the message (`requeue: true`) and, after a threshold of consecutive
  Aborts, errors-out the whole Shovel for an operator to resolve.

_Avoid_: result, status, ack-mode (ack-mode is the separate
Confirmed/OnPublish/NoAck delivery-guarantee setting).
