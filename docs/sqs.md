# SQS

LavinMQ speaks the Amazon SQS API natively, so applications built on the
unmodified AWS SDKs (boto3, AWS SDK for Java v2, AWS SDK for JavaScript v3,
Go v2, .NET, Ruby v3, PHP v3) and the `aws` CLI can use LavinMQ queues by
pointing the client at a LavinMQ endpoint URL. No plugin, client-side shim or
SDK patch is needed. Internally, SQS queues are ordinary durable AMQP queues,
so AMQP and SQS clients can publish to and consume from the same queue.

```python
import boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:9324",
    region_name="us-east-1",
    aws_access_key_id="guest",      # a LavinMQ user name
    aws_secret_access_key="guest",  # ignored, see Authentication
)
url = sqs.create_queue(QueueName="orders")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="hello")
for msg in sqs.receive_message(QueueUrl=url, WaitTimeSeconds=20).get("Messages", []):
    print(msg["Body"])
    sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
```

## Ports

| Protocol | Default Port | Config Key |
|----------|--------------|------------|
| SQS (HTTP) | 9324 | `port` in `[sqs]` |
| SQS over TLS | disabled (`-1`) | `tls_port` in `[sqs]` |

The SQS listener is separate from the management HTTP port. Like the other
listeners it binds to `127.0.0.1` by default; set `bind = ::` in the `[sqs]`
section to accept remote connections. Unix domain sockets are supported via
`unix_path`. See [Configuration](configuration.md#sqs-section).

## Wire protocol

LavinMQ implements the AWS JSON 1.0 protocol (`Content-Type:
application/x-amz-json-1.0`, `X-Amz-Target: AmazonSQS.<Action>`), which is
what the SDKs have used for SQS since 2023:

| SDK | JSON protocol since |
|-----|--------------------|
| boto3 / botocore | 1.28 / 1.31 |
| AWS SDK for Java v2 | 2.21 |
| AWS SDK for JavaScript v3 | 3.400 |
| AWS SDK for Go v2 | service/sqs 1.25 |
| AWS SDK for .NET | 3.7.200 |
| AWS SDK for Ruby v3 | aws-sdk-sqs 1.60 |
| AWS CLI | v2.13 |

Clients using the older AWS Query protocol (form-encoded requests, XML
responses) get an `InvalidAction` error in XML form telling them to upgrade.

## Authentication

SQS clients sign requests with AWS Signature Version 4. The **access key id of
the credential is used as the LavinMQ user name**; the secret access key and
the signature are not verified. LavinMQ stores only password hashes, and
SigV4 needs the plaintext secret to recompute a signature, so there is nothing
to verify against. Any non-empty string works as the secret.

This means the SQS listener authenticates by user name alone. Keep it on a
trusted network, or behind a proxy that authenticates, and use the user's
[permissions](users-permissions.md) to limit what each name can do. A request
without credentials is rejected with `MissingAuthenticationToken`, and an
unknown user with `InvalidClientTokenId`.

The user's per-vhost permission regexes are applied to the queue name:

| SQS actions | Permission |
|-------------|------------|
| `CreateQueue`, `DeleteQueue`, `SetQueueAttributes`, `TagQueue`, `UntagQueue` | configure |
| `SendMessage`, `SendMessageBatch` | write |
| `ReceiveMessage`, `DeleteMessage*`, `ChangeMessageVisibility*`, `PurgeQueue`, `GetQueueUrl`, `GetQueueAttributes`, `ListQueueTags` | read |
| `ListQueues` | any permission on the vhost; only readable queues are listed |

A denied request returns `AccessDeniedException` (HTTP 403).

## Queue URLs and vhosts

Queue URLs are minted by LavinMQ and have the form

```
http://<host>:<port>/<vhost>/<queue-name>
```

The default vhost `/` is written as the fake AWS account id `000000000000`, so
URLs look like the ones ElasticMQ and LocalStack produce. Any 12-digit account
id is accepted as an alias for the default vhost. Other vhosts appear
URI-encoded in the same position.

For actions that take a queue name instead of a URL (`CreateQueue`,
`GetQueueUrl`, `ListQueues`), the vhost is taken from the path of the endpoint
URL the SDK is configured with:

| Endpoint URL | Vhost |
|--------------|-------|
| `http://localhost:9324` | `/` |
| `http://localhost:9324/000000000000` | `/` |
| `http://localhost:9324/tenant-a` | `tenant-a` |

`GetQueueUrl` also honours `QueueOwnerAWSAccountId` as a vhost name. The host
and port of minted URLs come from the request's `Host` header; set
`public_url` in the `[sqs]` section to override them, for example behind a
load balancer.

## Mapping to AMQP

| SQS | LavinMQ |
|-----|---------|
| Queue | durable AMQP queue with the same name |
| `MessageRetentionPeriod` | `x-message-ttl` on queues created over SQS |
| `SendMessage` | publish through the default exchange with the queue name as routing key |
| `DelaySeconds` | publish through the internal `sqs.delayed` delayed-message exchange |
| `ReceiveMessage` | `basic.get` without auto-ack; long polling waits on the queue |
| `DeleteMessage` | `basic.ack` |
| visibility timeout expiry, `ChangeMessageVisibility` with `0` | `basic.reject` with requeue |
| `PurgeQueue` | queue purge |
| FIFO `MessageDeduplicationId` | `x-message-deduplication` with a 5 minute cache |

Message fields are stored in AMQP properties and headers, so AMQP consumers
can read them and AMQP publishers can set them:

| SQS field | AMQP property / header |
|-----------|------------------------|
| `MessageBody` | body, `content_type` `text/plain; charset=utf-8` |
| `MessageId` | `message_id` |
| `SentTimestamp` | header `x-sqs-sent-timestamp` (ms), `timestamp` (s) |
| `SenderId` | header `x-sqs-sender-id` |
| `MessageAttributes` | header `x-sqs-attributes`, a table of `{DataType, StringValue \| BinaryValue}` per attribute |
| `MessageGroupId` | header `x-sqs-message-group-id` |
| `MessageDeduplicationId` | header `x-sqs-deduplication-id` |
| `AWSTraceHeader` | header `x-sqs-trace-header` |

Messages published over AMQP without these are still received over SQS:
the body is returned as is, a message without `message_id` gets an id derived
from its position that stays the same across redeliveries, and
`SentTimestamp` falls back to the broker timestamp.

Queues declared over AMQP are visible to `GetQueueUrl`, `ListQueues` and
`ReceiveMessage` with default SQS attributes. `CreateQueue` on such a queue
adopts it and stores the given attributes.

## Queue attributes

SQS attributes and tags are stored per vhost in `sqs_queues.json` in the data
directory and replicated to followers.

| Attribute | Supported | Notes |
|-----------|-----------|-------|
| `VisibilityTimeout` | Yes | default 30 s, per-receive override supported |
| `ReceiveMessageWaitTimeSeconds` | Yes | default long poll wait |
| `DelaySeconds` | Yes | queue default, per-message override supported |
| `MaximumMessageSize` | Yes | body plus attributes, 1 KiB to 1 MiB |
| `MessageRetentionPeriod` | Partly | applied as `x-message-ttl` when the queue is created over SQS; changing it later only updates the reported value |
| `FifoQueue`, `ContentBasedDeduplication` | Yes | deduplication via the queue's deduplication cache; see FIFO below |
| `RedrivePolicy`, `RedriveAllowPolicy`, `Policy`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, `SqsManagedSseEnabled`, `DeduplicationScope`, `FifoThroughputLimit` | Stored | accepted and returned, not enforced. Use a dead-letter [policy](policies.md) on the queue for redrive |
| `ApproximateNumberOfMessages`, `ApproximateNumberOfMessagesNotVisible`, `CreatedTimestamp`, `LastModifiedTimestamp`, `QueueArn` | Yes | computed live. `ApproximateNumberOfMessagesDelayed` is always `0` |

The ARN uses the region from the client's credential:
`arn:aws:sqs:<region>:<vhost>:<queue>`.

## Messages in flight

A received message is an unacked message in the queue, so it shows up in the
`unacked` counts of the management UI and API. The receipt handle identifies
that delivery; when the visibility timeout passes without `DeleteMessage`,
the message is requeued and delivered again with an incremented
`ApproximateReceiveCount`, and the old receipt handle becomes invalid.

In-flight state is kept in memory on the node the client talks to. After a
restart or a leader change all unacked messages are requeued, so delivery is
at-least-once, the same guarantee SQS gives for standard queues.

## FIFO queues

Queues named `*.fifo` with `FifoQueue=true` require `MessageGroupId` on send,
deduplicate on `MessageDeduplicationId` (or the SHA-256 of the body with
`ContentBasedDeduplication`) for 5 minutes, and reject `DelaySeconds`.
Messages are delivered in the order they were sent. Unlike SQS, LavinMQ does
not block a message group while one of its messages is in flight, so
consumers that need strict per-group ordering must receive one message at a
time.

## Supported actions

`CreateQueue`, `GetQueueUrl`, `ListQueues`, `DeleteQueue`,
`GetQueueAttributes`, `SetQueueAttributes`, `PurgeQueue`, `TagQueue`,
`UntagQueue`, `ListQueueTags`, `SendMessage`, `SendMessageBatch`,
`ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`,
`ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`.

`AddPermission`, `RemovePermission`, `ListDeadLetterSourceQueues` and the
message move task actions return `UnsupportedOperation`.

## Configuration

```ini
[sqs]
bind = ::
port = 9324
tls_port = -1
; unix_path = /var/run/lavinmq-sqs.sock
; public_url = https://sqs.example.com
```

Set `port = -1` to disable the SQS listener. TLS uses the certificate
configured in `[main]`.
