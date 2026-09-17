# SQS proxy for LavinMQ — design and implementation plan

Status: proposal
Scope: let applications use the **unmodified AWS SQS SDKs** (boto3, AWS SDK for
Java v2, aws-sdk-js v3, Go v2, .NET, Ruby, PHP, the `aws` CLI) against LavinMQ
by pointing them at a LavinMQ endpoint URL. No SDK patches, no client-side
shims. LavinMQ speaks the SQS wire protocol and maps every SQS concept onto
existing AMQP infrastructure, the same way the MQTT server does.

```ruby
# Only the endpoint changes. Everything else is stock SDK code.
sqs = Aws::SQS::Client.new(endpoint: "http://lavinmq:9324", region: "us-east-1",
                           access_key_id: "AKIA...", secret_access_key: "...")
url = sqs.create_queue(queue_name: "orders").queue_url
sqs.send_message(queue_url: url, message_body: "hello")
msgs = sqs.receive_message(queue_url: url, wait_time_seconds: 20).messages
sqs.delete_message(queue_url: url, receipt_handle: msgs[0].receipt_handle)
```

---

## 1. Constraints that shape the design

1. **Two wire protocols, one API.** SQS SDKs released before mid-2023 use the
   *AWS Query* protocol (form-encoded `Action=SendMessage&Version=2012-11-05`
   POST, XML responses). Newer SDKs use *AWS JSON 1.0* (`Content-Type:
   application/x-amz-json-1.0`, `X-Amz-Target: AmazonSQS.SendMessage`, JSON
   body, `POST /`). "Unmodified SDK" means both must work, selected per request
   by content type / `X-Amz-Target`.
2. **SigV4 needs the plaintext secret.** Every SDK signs requests with
   AWS Signature Version 4, an HMAC chain over the secret key. LavinMQ stores
   only password hashes, so a user's password cannot be the secret key.
   SQS credentials have to be a separate, plaintext-at-rest *access key* object
   attached to a user (see §4).
3. **SDKs verify MD5 checksums.** boto3, Java and others verify
   `MD5OfMessageBody`, `MD5OfMessageAttributes` and
   `MD5OfMessageSystemAttributes` on `SendMessage`/`ReceiveMessage` responses
   and raise if they mismatch. The attribute MD5 uses a specific binary
   encoding that must be reproduced exactly.
4. **SDKs map errors by code.** Retry/backoff decisions, `QueueDoesNotExist`
   exception classes and the like are driven by the error `__type` /
   `<Code>` and the `x-amzn-query-error` header. Error shapes must be exact.
5. **Queue URLs are opaque to the SDK, but structured for us.** The server
   mints the URL in `CreateQueue`/`GetQueueUrl` and every later call passes it
   back. We use it to carry the vhost (§3.2).
6. **Repo rules.** No allocations in the publish/deliver hot paths; every
   change ships with specs; `make test` and `make lint` must pass.

---

## 2. Architecture

A new `LavinMQ::SQS` module in `src/lavinmq/sqs/`, structured after
`src/lavinmq/mqtt/`:

```
                 ┌──────────────────────────────────────────────┐
 SDK (HTTP) ───► │ SQS::Server (own ::HTTP::Server, port 9324)   │
                 │  ├ RequestIdHandler        x-amzn-RequestId  │
                 │  ├ SigV4Handler            AccessKeyStore    │
                 │  ├ ProtocolHandler         Query | JSON 1.0  │
                 │  └ ActionDispatcher  ─────► SQS::Broker(vhost)│
                 └──────────────────────────────────────────────┘
                                                  │
        ┌─────────────────────────────────────────┼─────────────────────┐
        ▼                                         ▼                     ▼
  VHost#publish (default exchange,        AMQP::Queue#basic_get /   SQS::QueueMeta store
  or sqs.delayed x-delayed-message)       #ack / #reject            (attrs, tags, per vhost)
                                                  ▲
                                          SQS::Inflight (receipt handles,
                                          visibility-timeout min-heap)
```

* **`SQS::Server`** — one `::HTTP::Server` with its own listeners (TCP, TLS,
  unix), bound by `Launcher#start_listeners` exactly like `MetricsServer`.
  Separate from the management HTTP server on purpose: queue URLs must be
  self-contained, and mgmt auth (Basic/cookie/OAuth) does not apply.
* **`SQS::Broker`** — per vhost (`SQS::Brokers` mirrors `MQTT::Brokers`),
  owns queue lookup/creation, send, receive, in-flight tracking and
  attribute metadata. Keeps action classes thin and protocol-agnostic.
* **`SQS::Inflight`** — per queue: receipt handle → `SegmentPosition`,
  plus a `MinHeap` of visibility deadlines driven by one fiber per broker.
* **Protocol layer** — parses both wire formats into one
  `SQS::Request` (`action : String`, `params : Hash(String, String)` with
  flattened Query-style keys, e.g. `MessageAttribute.1.Name`) and renders one
  `SQS::Response` into JSON or XML. Action code never sees the wire format.

Existing building blocks reused, nothing forked:

| SQS concept | LavinMQ primitive |
|---|---|
| Queue | durable `AMQP::Queue` in the vhost (`VHost#declare_queue`) |
| SendMessage | `VHost#publish(Message)` via default exchange, routing key = queue name |
| DelaySeconds | internal `sqs.delayed` exchange of type `x-delayed-message` (`x-delayed-type: direct`), `x-delay` header |
| ReceiveMessage | `Queue#basic_get(no_ack: false)` up to 10 times |
| Long polling | `select` on `queue.empty.when_false.receive` with `timeout WaitTimeSeconds` |
| DeleteMessage | `Queue#ack(sp)` |
| Visibility timeout expiry | `Queue#reject(sp, requeue: true)` (redelivered = true) |
| MessageRetentionPeriod | `x-message-ttl` |
| RedrivePolicy | `x-dead-letter-exchange` (default exchange) + `x-dead-letter-routing-key` + `x-delivery-limit` = maxReceiveCount |
| ApproximateReceiveCount | `x-delivery-count` header (present once a delivery limit is set; otherwise tracked in-flight) |
| FIFO dedup (`MessageDeduplicationId`) | `x-message-deduplication` + `x-deduplication-header` (existing `Deduplication` cache), 5-minute TTL |
| PurgeQueue | `Queue#purge` |
| ApproximateNumberOfMessages / NotVisible | `message_count` / `unacked_count` |
| Permissions | existing per-vhost user permissions: configure/write/read regexes |

---

## 3. Protocol mapping

### 3.1 Request parsing

Selection per request:

| Signal | Protocol |
|---|---|
| `Content-Type: application/x-amz-json-1.0` and `X-Amz-Target: AmazonSQS.<Action>` | JSON 1.0 |
| `Content-Type: application/x-www-form-urlencoded` with `Action=` | Query |
| anything else | 400 `InvalidAction` in Query shape |

Both are normalised to `SQS::Request`. JSON nested structures are flattened to
Query member syntax (`MessageAttributes.<name>.DataType` ↔
`MessageAttribute.1.Name/Value.DataType`) so each action has one
implementation. In Query, the request path may be the queue URL path
(old SDKs POST to `/<account>/<queue>`); `QueueUrl` in the body wins, the path
is the fallback.

### 3.2 Queue URLs and vhosts

```
http(s)://<host>:<port>/<vhost-segment>/<queue-name>
```

* `<vhost-segment>` is the URI-encoded vhost name (`/` → `%2F`).
* A 12-digit numeric segment (the fake AWS account id that many tools and
  ElasticMQ users hard-code, e.g. `000000000000`) is an alias for the default
  vhost `/`. Any 12-digit account id in a URL maps to `/`.
* `<host>:<port>` is taken from the request `Host` header so URLs work behind
  load balancers; `sqs_public_url` config can override it.
* Queue names follow SQS rules (1–80 chars `[A-Za-z0-9_-]`, `.fifo` suffix
  for FIFO) and are used verbatim as AMQP queue names.

### 3.3 Actions

Ordered by phase (see §7). Behaviour follows the SQS API reference
(`2012-11-05`).

| Action | Mapping / notes |
|---|---|
| `CreateQueue` | `declare_queue(name, durable: true, auto_delete: false, args)`; store attributes in `QueueMeta`. Existing queue with identical attributes → same URL (idempotent); different → `QueueNameExists`. `.fifo` ⇒ `FifoQueue=true` required and vice versa. |
| `GetQueueUrl` | lookup; `QueueDoesNotExist` otherwise. Works for queues created over AMQP too (they get default SQS attributes). |
| `ListQueues` | `QueueNamePrefix`, `MaxResults` (1–1000) + `NextToken` (opaque base64 of last name). |
| `DeleteQueue` | `vhost.delete_queue`; drop meta and in-flight. |
| `SendMessage` | validate body (≤ `MaximumMessageSize`, UTF-8, allowed XML chars), build `AMQP::Properties` (§3.4), publish. `DelaySeconds` > 0 or queue `DelaySeconds` ⇒ publish through `sqs.delayed`. Returns `MessageId`, `MD5OfMessageBody`, `MD5OfMessageAttributes`, `SequenceNumber` for FIFO. |
| `ReceiveMessage` | `MaxNumberOfMessages` 1–10, `WaitTimeSeconds` 0–20 (default from queue `ReceiveMessageWaitTimeSeconds`), `VisibilityTimeout` override, `AttributeNames`/`MessageSystemAttributeNames`, `MessageAttributeNames` (with `All`, `.*` and `prefix.*`). Empty result is `200` with no messages, never an error. |
| `DeleteMessage` | decode receipt handle → `ack`. Unknown/expired handle ⇒ `ReceiptHandleIsInvalid`. Handle for a message already redelivered (generation mismatch) ⇒ `InvalidParameterValue` "The receipt handle has expired". |
| `ChangeMessageVisibility` | 0–43200 s; re-key the heap entry. `0` makes the message immediately visible (requeue). |
| `SendMessageBatch` / `DeleteMessageBatch` / `ChangeMessageVisibilityBatch` | ≤ 10 entries, ≤ 256 KiB total; per-entry success/failure lists (`BatchResultErrorEntry` with `SenderFault`). Distinct-id / too-many-entries / empty-batch errors per spec. |
| `GetQueueAttributes` | `All` or list. Live: `ApproximateNumberOfMessages`, `...NotVisible`, `...Delayed` (delayed exchange queue count for this routing key), `CreatedTimestamp`, `LastModifiedTimestamp`, `QueueArn` (`arn:aws:sqs:<region>:<vhost-segment>:<name>`), plus stored attributes. |
| `SetQueueAttributes` | proxy-enforced attributes update meta only; storage attributes (`MessageRetentionPeriod`, `RedrivePolicy`, `MaximumMessageSize`) are applied via a per-queue operator-style policy `sqs.<queue>` with pattern `^\Q<name>\E$` (queue arguments are immutable in AMQP; the policy mechanism already re-applies TTL/DLX/limits live). |
| `PurgeQueue` | `purge`; 60-second `PurgeQueueInProgress` throttle per spec. |
| `TagQueue` / `UntagQueue` / `ListQueueTags` | stored in `QueueMeta`. |
| `ListDeadLetterSourceQueues` | scan meta for `RedrivePolicy.deadLetterTargetArn == this`. |
| `AddPermission` / `RemovePermission` | accepted and stored (no-op enforcement; documented). |
| `StartMessageMoveTask`, `ListMessageMoveTasks`, `CancelMessageMoveTask` | phase 3 via an internal shovel; until then `UnsupportedOperation`. |

Anything else: `InvalidAction`.

### 3.4 Message mapping

Publish side (`SendMessage`) builds a `Message` with:

| SQS | AMQP property / header |
|---|---|
| `MessageBody` | body (`content_type = text/plain; charset=utf-8`) |
| `MessageId` (UUID v4 minted by server) | `message_id` |
| `SentTimestamp` | `timestamp` (ms kept in header `x-sqs-sent-timestamp` since AMQP timestamp is seconds) |
| `MessageAttributes` | `headers["x-sqs-attributes"]` = `AMQP::Table{ name => Table{ "DataType" => "String"\|"Number"\|"Binary"\|"<type>.custom", "StringValue" => str \| "BinaryValue" => Bytes } }` — lossless round trip, readable by AMQP consumers |
| `MessageSystemAttributes.AWSTraceHeader` | `headers["x-sqs-trace-header"]` |
| `MessageGroupId` | `headers["x-sqs-message-group-id"]` |
| `MessageDeduplicationId` | `headers["x-sqs-deduplication-id"]` (queue declared with `x-deduplication-header` pointing here) |
| `DelaySeconds` | `headers["x-delay"]` (ms) when routed via `sqs.delayed` |
| `SenderId` | `user_id` = access key id |
| — | `delivery_mode = 2`, persistent |

Receive side reverses the mapping. Messages published by plain AMQP clients
without these headers still work: `MessageId` falls back to `message_id`
property or, if absent, a stable hash of `(queue, segment_position)` so
that the id is identical across redeliveries; `SentTimestamp` falls back to
the message timestamp; body bytes are returned as UTF-8 string (invalid UTF-8
is rejected on receive with `InvalidMessageContents` … the message is
requeued, never lost).

`MD5OfMessageBody` = hex MD5 of the raw body. `MD5OfMessageAttributes`
follows the documented SQS encoding (for each attribute sorted by name:
4-byte BE length + name, 4-byte BE length + data type, 1 byte
transport type `1`=string/`2`=binary, 4-byte BE length + value). Both
implemented in `SQS::Checksums` with the AWS documentation vectors as specs.
Verified by the SDKs, so this must be bit-exact.

### 3.5 Receipt handles and visibility timeout

A receipt handle encodes `queue-name | segment_position | generation |
random-nonce`, base64url. `generation` increments every time the message is
handed out again so a stale handle cannot ack a newer delivery.
`SQS::Inflight` holds, per queue:

* `Hash(handle, Entry{sp, deadline, first_receive_ts, receive_count})`
* `MinHeap` of `(deadline, handle)` for expiry

One fiber per `SQS::Broker` pops due entries and calls `queue.reject(sp,
requeue: true)`. Because unacked messages are already tracked by the queue's
`unacked_count`, the management UI and metrics stay correct. On server
restart or leader failover the in-flight table is gone and the message store
requeues unacked messages, so semantics are at-least-once — identical to what
SQS promises for standard queues.

In-flight entries also register through `Queue#basic_get_unacked_push` with a
lightweight `SQS::Channel` (a `Client::Channel` subclass) so they show up in
`/api/queues/:vhost/:name/unacked`, and the SQS connection appears in
`/api/connections` with `protocol: "sqs"`.

### 3.6 Long polling

```crystal
select
when queue.empty.when_false.receive  then retry basic_get
when timeout wait_time                then return []
when @closed.when_true.receive        then return []
end
```

No polling loop, no timers per request beyond the `select` timeout. If
`basic_get` still finds nothing (another receiver won the race) the loop
re-arms until the deadline.

### 3.7 Errors

JSON 1.0:

```
HTTP/1.1 400
x-amzn-RequestId: <uuid>
x-amzn-ErrorType: QueueDoesNotExist
x-amzn-query-error: AWS.SimpleQueueService.NonExistentQueue;Sender

{"__type":"com.amazonaws.sqs#QueueDoesNotExist","message":"The specified queue does not exist."}
```

Query:

```xml
<ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <Error><Type>Sender</Type><Code>AWS.SimpleQueueService.NonExistentQueue</Code>
  <Message>The specified queue does not exist.</Message></Error>
  <RequestId>…</RequestId>
</ErrorResponse>
```

One `SQS::Error` hierarchy carries both the modern `__type` name and the
legacy Query code, the HTTP status (400/403/404/500) and `Sender`/`Receiver`.
Auth failures use `403` with `InvalidClientTokenId` (unknown key),
`SignatureDoesNotMatch`, `AccessDeniedException` (permission regex),
`RequestExpired`/`InvalidSignatureException` (clock skew). Rate limiting via
the existing `RateLimiter` returns `RequestThrottled` so SDK backoff kicks in.

---

## 4. Authentication and authorization

### 4.1 Access keys

New object: **access key**, stored in `<data_dir>/sqs_access_keys.json`
through the existing `Persister`/replicator so it follows the leader in a
cluster and is included in definitions export/import.

```json
{"access_key_id": "LMQ7F3K2Q9V1XZ0P4B8C", "secret_access_key": "…40 chars…",
 "user": "alice", "created_at": "…", "description": "orders-service"}
```

* Secret is stored **plaintext** (SigV4 requires it). The file is written
  `0600`. This is exactly the AWS IAM model and how every SQS-compatible
  server (ElasticMQ, LocalStack, GoAWS) works; documented clearly.
* Many keys per user; deleting a user deletes its keys.
* Management: `GET/POST /api/users/:name/access-keys`,
  `DELETE /api/users/:name/access-keys/:id`, mirrored in `lavinmqctl
  create_access_key / delete_access_key / list_access_keys`, and a panel in
  the user page of the management UI. The secret is shown once at creation.
* Key ids are 20 uppercase alphanumerics prefixed `LMQ` so they pass SDK
  client-side validation and are visibly not AWS keys.

### 4.2 SigV4 verification (`SQS::SigV4`)

* Parse `Authorization: AWS4-HMAC-SHA256 Credential=<id>/<date>/<region>/sqs/aws4_request, SignedHeaders=…, Signature=…`
  (also the pre-signed query-string form for completeness).
* Rebuild the canonical request from method, URI path, canonical query,
  signed headers and the payload hash (`X-Amz-Content-Sha256` if present,
  else SHA-256 of the body; `UNSIGNED-PAYLOAD` accepted).
* Derive `kSigning` from the stored secret; compare with
  `Crypto::Subtle.constant_time_compare`.
* **Region and service are not validated** (anything the SDK is configured
  with works); date must be within ±15 minutes.
* Session tokens (`X-Amz-Security-Token`) are ignored unless a future
  STS-like feature lands.
* Signing key derivation is cached per `(key id, date, region)` for a day —
  this is the hot-path allocation to avoid.
* Config `sqs_auth = sigv4 | none`. `none` maps the access key id in the
  `Credential` (or, failing that, `sqs_default_user`) straight to a user
  without checking the signature. Off by default; intended for local dev
  and drop-in ElasticMQ replacement.

### 4.3 Authorization

The resolved `Auth::User` and vhost (from the queue URL) go through the
existing permission regexes:

| Action group | Permission |
|---|---|
| `CreateQueue`, `DeleteQueue`, `SetQueueAttributes`, `PurgeQueue`, `Tag*`, `*Permission` | configure |
| `SendMessage*` | write |
| `ReceiveMessage`, `DeleteMessage*`, `ChangeMessageVisibility*`, `GetQueueAttributes`, `GetQueueUrl`, `ListQueueTags` | read |
| `ListQueues` | vhost access; results filtered by read regex |

Users without any permission on the vhost get `AccessDeniedException`.
Existing vhost limits (max queues, max connections) are honoured; the SQS
listener counts as one connection per access key per vhost for the UI.

---

## 5. Configuration

`src/lavinmq/config/options.cr`, new `[sqs]` ini section:

| Key | CLI | Default | Notes |
|---|---|---|---|
| `bind` | `--sqs-bind` | `127.0.0.1` | follows `bind` in `[main]` like MQTT/HTTP |
| `port` | `--sqs-port` | `9324` | `-1` disables (same as ElasticMQ default port, so existing local setups just work) |
| `tls_port` | `--sqss-port` | `-1` | uses the shared TLS context (`create_tls_context`) and SNI |
| `unix_path` | `--sqs-unix-path` | `""` | |
| `auth` | | `sigv4` | `sigv4` or `none` |
| `default_user` | | `""` | used by `auth = none` when no `Credential` header |
| `public_url` | | `""` | scheme+host+port to mint queue URLs with; default derived from `Host` header |
| `max_message_size` | | `262144` | upper bound for `MaximumMessageSize` |
| `max_inflight_per_queue` | | `120000` | SQS's own limit; `OverLimit` beyond it |

Launcher wiring: `@sqs_server = SQS::Server.new(server, config)` and
`bind_listeners(sqs_server, …)` in `start_listeners`; close in `stop` before
`@server`. `Launcher#initialize` gets a fourth TLS context.

---

## 6. File layout

```
src/lavinmq/sqs.cr                      # requires
src/lavinmq/sqs/
  CONTEXT.md                            # vocabulary, like shovel/CONTEXT.md
  server.cr                             # ::HTTP::Server, listeners, handler chain
  brokers.cr / broker.cr                # per-vhost façade (like mqtt/brokers.cr)
  request.cr / response.cr              # protocol-neutral request/response
  protocol/json.cr                      # JSON 1.0 parse + render
  protocol/query.cr                     # Query parse + XML render
  errors.cr                             # SQS::Error hierarchy (+ legacy codes)
  sigv4.cr                              # canonical request, key derivation, verify
  access_key.cr / access_key_store.cr   # persisted keys
  auth_handler.cr                       # resolves user or raises 403
  queue_url.cr                          # mint/parse, vhost segment rules
  queue_meta.cr / queue_meta_store.cr   # SQS attributes + tags per queue, replicated
  message_mapping.cr                    # SQS <-> AMQP::Properties (+ attributes table)
  checksums.cr                          # MD5 of body / attributes
  inflight.cr                           # receipt handles + visibility heap
  channel.cr                            # SQS::Channel < Client::Channel (unacked listing)
  connection.cr                         # SQS::Connection < Client (connections listing)
  actions/                              # one file per action, `Action.call(broker, req) : Response`
    create_queue.cr … change_message_visibility_batch.cr
src/lavinmq/http/controller/access_keys.cr
src/lavinmqctl/… access key commands
docs/sqs.md                             # user documentation (ports, auth, mapping table, limits)
```

Specs:

```
spec/sqs/
  spec_helper.cr                        # with_sqs_server { |sqs, s| }, tiny SigV4 signer + JSON/Query clients
  sigv4_spec.cr                         # AWS SigV4 test-suite vectors (get-vanilla, post-x-www-form-urlencoded, …)
  checksums_spec.cr                     # MD5OfMessageAttributes vectors from AWS docs
  protocol/json_spec.cr, protocol/query_spec.cr
  queue_url_spec.cr, message_mapping_spec.cr, inflight_spec.cr
  actions/*_spec.cr                     # one per action, both protocols where shape differs
  access_keys_api_spec.cr               # HTTP API + lavinmqctl
  integrations/                         # real SDK smoke tests (boto3, aws-sdk-js v3, aws cli),
                                        # run in a separate GitHub Actions job, like spec/mqtt/integrations
```

The SDK integration job is the acceptance test for the headline requirement:
each SDK runs create → send (with attributes, checks MD5) → receive with long
poll → change visibility → delete → batch → purge → delete queue, plus one
run with the CLI (`aws --endpoint-url … sqs …`).

---

## 7. Phases

Each phase is independently shippable and behind `sqs_port` (default on
only from phase 2).

**Phase 1 — skeleton and happy path (JSON 1.0, boto3 ≥ 1.28 / aws-sdk-js v3 work)**
- config, listener, launcher wiring, request id, error shapes
- access keys store + HTTP API; SigV4 verify; `auth = none`
- `CreateQueue`, `GetQueueUrl`, `ListQueues`, `DeleteQueue`,
  `SendMessage`, `ReceiveMessage` (no long poll, fixed 30 s visibility),
  `DeleteMessage`, `GetQueueAttributes` (approximate counts)
- body MD5, message ids, queue URL/vhost mapping
- docs/sqs.md first version

**Phase 2 — full standard-queue semantics (all SDK generations)**
- Query protocol + XML (older boto3/Java v1/Ruby v2) → both protocols
  covered by the integration job
- visibility timeout heap, `ChangeMessageVisibility`, long polling
- message attributes + `MD5OfMessageAttributes`, system attributes
- `DelaySeconds` (message and queue level) via `sqs.delayed`
- all three batch actions, `PurgeQueue`, `SetQueueAttributes` via
  per-queue policy, `RedrivePolicy` → DLX + delivery limit,
  `ListDeadLetterSourceQueues`, tags
- rate limiting → `RequestThrottled`; unacked/connections visible in UI
- flip `sqs_port` default to `9324`

**Phase 3 — FIFO and operations**
- FIFO queues: `ContentBasedDeduplication` (SHA-256 of body as dedup id),
  `MessageDeduplicationId` through the existing deduplication cache,
  `SequenceNumber`, `ReceiveRequestAttemptId`. Per-message-group in-flight
  exclusivity is approximated: one group at a time is blocked while any of
  its messages is in flight (documented deviation; exact SQS semantics need
  per-group sub-queues and are a follow-up)
- `StartMessageMoveTask` via an internal shovel from DLQ back to source
- clustering: `sqs_access_keys.json` and `QueueMeta` replicated; in-flight
  state is per-leader (documented)
- Prometheus metrics (`lavinmq_sqs_requests_total{action,code}`),
  management UI: access keys panel, `protocol: sqs` badge on connections
- `lavinmqperf sqs` mode

---

## 8. Decisions to confirm

1. **Own port (9324) vs. path under the management port.** Own port
   recommended: clean auth separation, ElasticMQ drop-in default, TLS/SNI per
   protocol like AMQP/MQTT.
2. **Vhost in the URL account-id slot** (with 12-digit ids aliasing `/`)
   vs. binding a vhost per access key. URL slot recommended: one key can
   reach several vhosts and URLs stay self-describing; permissions still
   gate access.
3. **Mutable storage attributes through a per-queue policy** vs. re-declare
   on change. Policy recommended: live re-application already exists;
   documented as a reserved `sqs.<queue>` policy name.
4. **Plaintext secrets at rest** (required by SigV4). Alternative is
   `auth = none`. No third option exists without an SDK-side change.
5. **Default `sqs_port`**: `-1` until phase 2, then `9324`.

## 9. Non-goals

- IAM policies, STS, resource policies beyond storing `AddPermission`.
- Exact FIFO per-group blocking (phase 3 approximation documented).
- SQS-managed server-side encryption attributes (`KmsMasterKeyId`) — accepted
  and ignored, since storage encryption is a LavinMQ deployment concern.
- SQS Extended Client (S3 offload) — client-side library, needs S3.
