import os
import uuid

import boto3
import pytest
from botocore.config import Config


@pytest.fixture(scope="session")
def sqs():
    # boto3 verifies the MD5 checksums of every SendMessage/ReceiveMessage
    # response, so this client exercises the checksum path too.
    return boto3.client(
        "sqs",
        endpoint_url=os.environ.get("SQS_ENDPOINT_URL", "http://127.0.0.1:9324"),
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("SQS_USER", "guest"),
        aws_secret_access_key="ignored",
        config=Config(retries={"max_attempts": 2}),
    )


@pytest.fixture
def queue_url(sqs):
    name = f"sdk-{uuid.uuid4().hex[:12]}"
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    yield url
    sqs.delete_queue(QueueUrl=url)
