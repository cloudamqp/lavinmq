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
AMQP queues (an exchange source is consumed through a temporary queue). The
Source owns consume and settlement; it never decides *whether* a message
succeeded.
_Avoid_: origin, input, upstream.

**Destination**:
Where a Shovel delivers messages (AMQP exchange/queue or HTTP endpoint). A
Destination delivers a message and reports an **Outcome**; it never touches the
Source.
_Avoid_: sink, target, output, downstream.

**Runner**:
The single fiber that owns a Shovel's run loop and its **policy**: it maps each
**Outcome** to a Source action and owns requeue timing, backoff, and the abort
threshold. Only this fiber starts or stops a Source or Destination. Outcomes
that arrive once the Source is stopped (confirms voided while the Shovel is
pausing or terminating) are ignored: there is nothing left to settle. The
Source classifies nothing. (An HTTP Destination retries a request
once on a fresh connection when a kept-alive connection turns out to be dead,
but it owns no Source policy.)
_Avoid_: worker, driver, supervisor.

**MultiDestinationHandler**:
The failover Destination wrapping a Shovel's list of `dest-uri`s. Holds one
*active* destination at a time; on an **Abort** outcome, a failure to start, or
a run of **Retry** outcomes it advances to the next. The outcome only *requests*
the failover; the next push, on the **Runner**'s fiber, carries it out. Stopping
a destination from its own confirm fiber would deadlock on the connection
close, and stopping it requeues (via Retry) everything still in flight on it.
It emits Abort upward only once every destination has aborted in a row with no
other outcome in between, and keeps advancing even then. Every start begins
with the first destination in the list, and start raises if none can be
started, so the Runner reconnects.
Name kept for continuity — it is a failover handler, not fan-out or
load-balancing.
_Avoid_: RandomDestination, load-balancer, fan-out, round-robin.

**Outcome**:
The per-message disposition a Destination reports back to the Runner. The
Destination maps its native result (HTTP status, AMQP confirm) to one of these;
the Runner decides what each one does. One of:

- **Confirmed** — delivered. Runner acks the message and resets failure counters.
- **Retry** — transient failure (HTTP 5xx/429/408/timeout/connection-refused;
  AMQP nack such as reject-publish overflow). Runner requeues (`requeue: true`)
  and retries with backoff, unbounded.
- **Reject** — the *message* is unacceptable (HTTP 400/422, or 413/415 and
  the other statuses about the request's size, type or headers). Runner rejects
  without requeue (`requeue: false`) so the source queue's dead-letter exchange
  handles it, then continues with the next message.
- **Abort** — the *destination* is unusable (HTTP 404, auth failure). Runner
  keeps the message (`requeue: true`) and, after a threshold of consecutive
  Aborts, moves the Shovel to the **Aborted** state for an operator to resolve.

_Avoid_: result, status, ack-mode (ack-mode is the separate
OnConfirm/OnPublish/NoAck delivery-guarantee setting).

**Aborted** (state):
The terminal state a Runner enters once the abort threshold is crossed: the
Destination is unusable, the Shovel stays put with the reason in `error`, and it
does not reconnect until it is resumed or its parameter is recreated. Distinct
from **Error**, the transient state of a Shovel that is about to reconnect with
backoff.
_Avoid_: errored-out, failed, dead.
