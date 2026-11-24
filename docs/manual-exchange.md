# Manual Exchanges

A manual exchange does not route messages by itself. It holds each message until a client decides where the message goes. The client that decides is a third party: it is not the publisher, and it is not a consumer of the destination queues.

## How It Works

1. Declare an exchange of any routing type (`direct`, `topic`, `fanout`, `headers`) with the argument `x-manual-exchange: true`, and bind queues to it as usual.
2. LavinMQ creates an internal pending queue, `amq.manual-<exchange_name>`.
3. Each message that is published to the exchange goes into the pending queue, in order. The bindings are not used yet.
4. A client consumes the pending queue (`basic.consume` or `basic.get`, with manual acks).
5. For each message that it holds, the client publishes a decision to the exchange. LavinMQ routes the stored message as the decision says and acks it in the pending queue, in one step.

## Declaration

```
Exchange type: direct  (or topic, fanout, headers)
Arguments:
  x-manual-exchange: true
  x-manual-max-length: 9                  (optional, limit of the pending queue)
  alternate-exchange: unroutable          (optional, receives the decision "none")
```

With `x-manual-max-length`, the pending queue uses `x-overflow: reject-publish`. When it is full, LavinMQ nacks new publishes on channels in confirm mode.

An exchange can't be both manual and delayed. An `x-mqtt-topic` or `x-federation-upstream` exchange can't be manual.

## Decisions

A decision is a `basic.publish` to the manual exchange, on the channel that holds the message, with two headers:

| Header | Value |
|--------|-------|
| `x-manual-delivery-tag` | The delivery tag of the message, as the pending queue delivered it to this channel |
| `x-manual-route` | A queue name, `none` or `auto` |

The body and the routing key of the decision are not used. LavinMQ routes the stored message, so the deciding client cannot change it.

| `x-manual-route` | Result |
|------------------|--------|
| A queue name | The message goes to that queue, if a binding of the exchange leads to it. |
| A queue that no binding leads to | Handled as `none`. |
| `none` | The message is unroutable. It goes to the alternate exchange, if there is one. Else LavinMQ drops it and counts it as unroutable. |
| `auto` | The exchange routes the message with its own bindings. |

LavinMQ carries out a wrong decision. It adds these headers to the routed message, so that consumers can find out:

| Header | Value |
|--------|-------|
| `x-manual-route` | The decision |
| `x-manual-match` | `true` if the decision gives the same queues as the bindings would |
| `x-manual-routed-by` | The connection name of the deciding client, or its user name |

Rules:

- A decision for a delivery tag that the channel does not hold, that is acked already, or that is not from the exchange's pending queue, closes the channel with `PRECONDITION_FAILED`, like `basic.ack` with an unknown tag. The channel's unacked messages go back to the pending queue.
- A decision that a destination queue refuses (for example a full queue with `reject-publish`) is nacked, and the message stays unacked.
- A decision can't be made in a transaction.
- `basic.ack` without a decision removes the message from the pending queue. `basic.reject` and `basic.nack` with requeue put it back for another client.
- If a client disconnects, its unacked messages go back to the pending queue.
- LavinMQ removes the decision headers from normal publishes, so a publisher cannot decide for its own messages.

## Permissions

- To publish to the exchange, a user needs write permission on it.
- To decide, a user needs read permission on the pending queue, so that it can consume. A decision needs no write permission on the exchange: it is allowed like an ack, by the delivery that it names. A user without write permission can't publish anything else to the exchange.
- Clients can consume from the pending queue and ack, reject or nack its messages. They can't bind, purge or delete it, or publish to it through the default exchange.

## Example

Three roles, three users:

| User | configure | write | read |
|------|-----------|-------|------|
| producer | | `^mailroom$` | |
| router | | | `^amq\.manual-mailroom$` |
| consumer | | | `^(workshop\|kitchen)$` |

The router:

```
basic.qos       prefetch_count=9
basic.consume   queue=amq.manual-mailroom
<- basic.deliver delivery_tag=17 routing_key=toy
basic.publish   exchange=mailroom
                headers: x-manual-delivery-tag=17, x-manual-route=workshop
```
