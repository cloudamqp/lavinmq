# Shovel delivery outcomes

A shovel moves messages from a **source** to a **destination**. For each message
the destination classifies the delivery attempt into an **Outcome**, and the
runner turns that outcome into an action on the source. Classification is
hardcoded per destination type; the runner owns the policy (acking, requeueing,
backoff and the error-out threshold).

See [`src/lavinmq/shovel/CONTEXT.md`](../src/lavinmq/shovel/CONTEXT.md) for the
vocabulary.

## Outcome → source action

The runner maps every outcome to exactly one action:

| Outcome | Source action | Effect |
| --- | --- | --- |
| `Confirmed` | `ack` | Message delivered and removed from the source. Resets the failure and abort counters. |
| `Retry` | `reject(requeue: true)` | Transient failure. Message stays on the source and is retried with capped exponential backoff. Retries are unbounded. |
| `Reject` | `reject(requeue: false)` | The message itself is unacceptable. It is dead-lettered via the source queue's DLX (dropped if no DLX is configured). The shovel continues. |
| `Abort` | `reject(requeue: true)` | The destination is unusable. The message is kept on the source; after `ABORT_THRESHOLD` (10) consecutive aborts the shovel errors out for an operator to resolve. |

## HTTP destination

`HTTPDestination#classify(response)` maps the HTTP status (or a transport error)
to an outcome:

| Condition | Outcome |
| --- | --- |
| `200`–`299` | `Confirmed` |
| `408`, `429`, `500`–`599` | `Retry` |
| `400`, `422` | `Reject` |
| Any other non-2xx (`401`, `403`, `404`, `405`, `410`, `3xx`, `418`, …) | `Abort` |
| Transport failure during the request (connection refused/reset, read timeout, or TLS handshake error — `IO::Error` / `OpenSSL::SSL::Error`) | `Retry` |

There is no in-place retry of a failed request: every outcome is reported to
the runner immediately, and a `Retry` makes the runner requeue the message and
apply its own capped backoff. The one exception is a kept-alive connection the
endpoint has silently closed. That is only detectable by the next request dying
on it (EOF or a reset, on a connection that already served a request), so that
request is retried once on a fresh connection before anything is reported. A
timeout, a refused connection, or a failure on a fresh connection is not retried
in place.

The body is sent with a `Content-Length` header (its size is known), not with
chunked transfer encoding.

## AMQP destination

`AMQPDestination` reports the broker's publisher-confirm result:

| Condition | Outcome |
| --- | --- |
| Publisher-confirm **ack** | `Confirmed` |
| Publisher-confirm **nack** (e.g. `x-overflow: reject-publish` on a full destination queue) | `Retry` |
| Connection / channel error mid-publish | Not an outcome — the exception is handled by the runner's reconnect loop. A `404` channel-close ("queue deleted") stops the shovel. |

## Ack-mode interaction

The shovel's `ack-mode` controls **when** an outcome is reported:

| Ack mode | AMQP destination | HTTP destination |
| --- | --- | --- |
| `on-confirm` | Reported on the asynchronous publisher-confirm callback (`Confirmed` / `Retry`). | Reported after the HTTP response (`classify`). |
| `on-publish` | `Confirmed` reported immediately after publishing, without waiting for a confirm. | Posts once and reports `Confirmed` as soon as the request returns, regardless of status; only a transport failure reports `Retry`. No status classification or in-place retry. |
| `no-ack` | No outcome is reported — the source consumes with `no-ack`, so there is nothing to settle. | No outcome is reported. |

## Failover across multiple destinations

When a shovel is configured with more than one `dest-uri`, the
`MultiDestinationHandler` provides coarse, ordered failover. One destination is
active at a time and it intercepts that destination's outcome:

- `Confirmed` / `Retry` / `Reject` — forwarded to the runner unchanged (any
  non-abort resets the failover cycle).
- `Abort` — advance to the next destination and retry the message there
  (forwarded as `Retry`). Only once **every** destination has aborted in a row,
  with no `Confirmed` in between, is `Abort` propagated so the runner errors out.
- No usable destination (none could start) — reports `Abort` rather than
  silently dropping the message.

## Notes

- A `Reject` with no dead-letter exchange configured on the source queue drops
  the message silently. Surfacing a UI warning for this is tracked separately.
- An in-flight HTTP request cannot be cancelled (closing the client does not
  interrupt it), so on pause the current request drains under its read timeout
  while no new deliveries are started; the in-flight message is redelivered on
  resume (the source is at-least-once).
