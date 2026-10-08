"""End-to-end tests with the unmodified AWS SDK (boto3) against a running
LavinMQ. Run with: make test-sqs-sdk (SQS_ENDPOINT_URL to override)."""

import time

import pytest
from botocore.exceptions import ClientError


def receive(sqs, url, **kw):
    return sqs.receive_message(QueueUrl=url, **kw).get("Messages", [])


def test_create_get_list_delete_queue(sqs):
    url = sqs.create_queue(QueueName="sdk-lifecycle")["QueueUrl"]
    assert url.endswith("/000000000000/sdk-lifecycle")
    assert sqs.get_queue_url(QueueName="sdk-lifecycle")["QueueUrl"] == url
    assert url in sqs.list_queues(QueueNamePrefix="sdk-life")["QueueUrls"]
    sqs.delete_queue(QueueUrl=url)
    with pytest.raises(sqs.exceptions.QueueDoesNotExist):
        sqs.get_queue_url(QueueName="sdk-lifecycle")


def test_send_receive_delete(sqs, queue_url):
    sent = sqs.send_message(QueueUrl=queue_url, MessageBody="hello from boto3")
    msgs = receive(sqs, queue_url, MessageSystemAttributeNames=["All"])
    assert len(msgs) == 1
    msg = msgs[0]
    assert msg["MessageId"] == sent["MessageId"]
    assert msg["Body"] == "hello from boto3"
    assert msg["Attributes"]["ApproximateReceiveCount"] == "1"
    assert msg["Attributes"]["SenderId"] == "guest"
    assert receive(sqs, queue_url) == []
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
    with pytest.raises(ClientError) as e:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
    assert e.value.response["Error"]["Code"] == "ReceiptHandleIsInvalid"


def test_message_attributes_checksums(sqs, queue_url):
    attrs = {
        "color": {"DataType": "String", "StringValue": "red"},
        "count": {"DataType": "Number", "StringValue": "42"},
        "blob": {"DataType": "Binary", "BinaryValue": b"\x00\x01\xff"},
    }
    sent = sqs.send_message(QueueUrl=queue_url, MessageBody="attrs", MessageAttributes=attrs)
    assert len(sent["MD5OfMessageAttributes"]) == 32
    msg = receive(sqs, queue_url, MessageAttributeNames=["All"])[0]
    assert msg["MessageAttributes"]["color"]["StringValue"] == "red"
    assert msg["MessageAttributes"]["count"]["DataType"] == "Number"
    assert msg["MessageAttributes"]["blob"]["BinaryValue"] == b"\x00\x01\xff"
    assert msg["MD5OfMessageAttributes"] == sent["MD5OfMessageAttributes"]


def test_visibility_timeout_and_change(sqs, queue_url):
    sqs.send_message(QueueUrl=queue_url, MessageBody="x")
    first = receive(sqs, queue_url, VisibilityTimeout=1)[0]
    assert receive(sqs, queue_url) == []
    second = receive(sqs, queue_url, WaitTimeSeconds=3, AttributeNames=["ApproximateReceiveCount"])[0]
    assert second["MessageId"] == first["MessageId"]
    assert second["Attributes"]["ApproximateReceiveCount"] == "2"
    sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=second["ReceiptHandle"], VisibilityTimeout=0)
    assert len(receive(sqs, queue_url)) == 1


def test_long_polling(sqs, queue_url):
    started = time.monotonic()
    assert receive(sqs, queue_url, WaitTimeSeconds=1) == []
    assert time.monotonic() - started >= 0.9


def test_delay_seconds(sqs, queue_url):
    sqs.send_message(QueueUrl=queue_url, MessageBody="later", DelaySeconds=1)
    assert receive(sqs, queue_url) == []
    assert receive(sqs, queue_url, WaitTimeSeconds=5)[0]["Body"] == "later"


def test_batches(sqs, queue_url):
    result = sqs.send_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": "a", "MessageBody": "one"}, {"Id": "b", "MessageBody": "two"}, {"Id": "c", "MessageBody": ""}],
    )
    assert [e["Id"] for e in result["Successful"]] == ["a", "b"]
    assert result["Failed"][0]["Id"] == "c"
    msgs = receive(sqs, queue_url, MaxNumberOfMessages=10)
    assert len(msgs) == 2
    deleted = sqs.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)],
    )
    assert len(deleted["Successful"]) == 2


def test_queue_attributes_and_tags(sqs, queue_url):
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "5"})
    sqs.send_message(QueueUrl=queue_url, MessageBody="x")
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
    assert attrs["VisibilityTimeout"] == "5"
    assert attrs["ApproximateNumberOfMessages"] == "1"
    assert attrs["QueueArn"].startswith("arn:aws:sqs:us-east-1:000000000000:")
    sqs.tag_queue(QueueUrl=queue_url, Tags={"env": "ci"})
    assert sqs.list_queue_tags(QueueUrl=queue_url)["Tags"] == {"env": "ci"}
    sqs.purge_queue(QueueUrl=queue_url)
    assert sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]["ApproximateNumberOfMessages"] == "0"


def test_fifo_queue(sqs):
    url = sqs.create_queue(QueueName="sdk-orders.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
    try:
        for dedup, body in [("d1", "first"), ("d1", "dup"), ("d2", "second")]:
            sqs.send_message(QueueUrl=url, MessageBody=body, MessageGroupId="g", MessageDeduplicationId=dedup)
        bodies = [m["Body"] for m in receive(sqs, url, MaxNumberOfMessages=10)]
        assert bodies == ["first", "second"]
    finally:
        sqs.delete_queue(QueueUrl=url)


def test_unknown_user_is_rejected(sqs):
    import boto3

    bad = boto3.client("sqs", endpoint_url=sqs.meta.endpoint_url, region_name="us-east-1",
                       aws_access_key_id="nobody", aws_secret_access_key="x")
    with pytest.raises(ClientError) as e:
        bad.list_queues()
    assert e.value.response["Error"]["Code"] == "InvalidClientTokenId"
