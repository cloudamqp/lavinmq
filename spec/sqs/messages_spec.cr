require "./spec_helper"

private def receive(sqs, url, **params)
  sqs.json("ReceiveMessage", {QueueUrl: url}.merge(params))["Messages"]?.try(&.as_a) || [] of JSON::Any
end

describe "SQS messages" do
  it "sends, receives and deletes a message" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("msgs")
      sent = sqs.json("SendMessage", {QueueUrl: url, MessageBody: "hello world"})
      sent["MessageId"].as_s.size.should eq 36
      sent["MD5OfMessageBody"].as_s.should eq Digest::MD5.hexdigest("hello world")
      sent["MD5OfMessageAttributes"]?.should be_nil

      msgs = receive(sqs, url, MessageSystemAttributeNames: ["All"])
      msgs.size.should eq 1
      msg = msgs[0]
      msg["MessageId"].as_s.should eq sent["MessageId"].as_s
      msg["Body"].as_s.should eq "hello world"
      msg["MD5OfBody"].as_s.should eq sent["MD5OfMessageBody"].as_s
      msg["ReceiptHandle"].as_s.should_not be_empty
      attrs = msg["Attributes"]
      attrs["ApproximateReceiveCount"].as_s.should eq "1"
      attrs["SenderId"].as_s.should eq "guest"
      attrs["SentTimestamp"].as_s.to_i64.should be_close(Time.utc.to_unix_ms, 5000)
      attrs["ApproximateFirstReceiveTimestamp"].as_s.to_i64.should be_close(Time.utc.to_unix_ms, 5000)

      q = s.vhosts["/"].queue("msgs")
      q.unacked_count.should eq 1
      receive(sqs, url).should be_empty # in flight
      sqs.json("DeleteMessage", {QueueUrl: url, ReceiptHandle: msg["ReceiptHandle"].as_s})
      q.unacked_count.should eq 0
      q.message_count.should eq 0
      sqs.error("DeleteMessage", {QueueUrl: url, ReceiptHandle: msg["ReceiptHandle"].as_s})[1].should eq "ReceiptHandleIsInvalid"
    end
  end

  it "returns an empty object when there is nothing to receive" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("empty")
      sqs.json("ReceiveMessage", {QueueUrl: url}).as_h.should be_empty
    end
  end

  it "receives up to MaxNumberOfMessages" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("many")
      15.times { |i| sqs.json("SendMessage", {QueueUrl: url, MessageBody: "m#{i}"}) }
      receive(sqs, url, MaxNumberOfMessages: 10).size.should eq 10
      receive(sqs, url, MaxNumberOfMessages: 10).size.should eq 5
      receive(sqs, url).should be_empty
      sqs.error("ReceiveMessage", {QueueUrl: url, MaxNumberOfMessages: 11})[1].should eq "InvalidParameterValue"
      sqs.error("ReceiveMessage", {QueueUrl: url, MaxNumberOfMessages: 0})[1].should eq "InvalidParameterValue"
    end
  end

  it "makes messages visible again after the visibility timeout" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("vis", {VisibilityTimeout: "1"})
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "again"})
      first = receive(sqs, url, AttributeNames: ["ApproximateReceiveCount"])
      first.size.should eq 1
      receive(sqs, url).should be_empty
      second = receive(sqs, url, WaitTimeSeconds: 3, AttributeNames: ["ApproximateReceiveCount"])
      second.size.should eq 1
      second[0]["MessageId"].should eq first[0]["MessageId"]
      second[0]["ReceiptHandle"].should_not eq first[0]["ReceiptHandle"]
      second[0]["Attributes"]["ApproximateReceiveCount"].as_s.should eq "2"
      # the old handle is no longer valid
      sqs.error("DeleteMessage", {QueueUrl: url, ReceiptHandle: first[0]["ReceiptHandle"].as_s})[1].should eq "ReceiptHandleIsInvalid"
      sqs.json("DeleteMessage", {QueueUrl: url, ReceiptHandle: second[0]["ReceiptHandle"].as_s})
    end
  end

  it "honours a per-receive VisibilityTimeout override" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("vis-override")
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "x"})
      receive(sqs, url, VisibilityTimeout: 1).size.should eq 1
      receive(sqs, url, WaitTimeSeconds: 3).size.should eq 1
    end
  end

  it "changes message visibility" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("chvis")
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "x"})
      handle = receive(sqs, url)[0]["ReceiptHandle"].as_s
      # extend, then make visible immediately
      sqs.json("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 100})
      receive(sqs, url).should be_empty
      sqs.json("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 0})
      msgs = receive(sqs, url, AttributeNames: ["ApproximateReceiveCount"])
      msgs.size.should eq 1
      msgs[0]["Attributes"]["ApproximateReceiveCount"].as_s.should eq "2"
      sqs.error("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 0})[1].should eq "ReceiptHandleIsInvalid"
      sqs.error("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: msgs[0]["ReceiptHandle"].as_s, VisibilityTimeout: 50_000})[1].should eq "InvalidParameterValue"
      sqs.error("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: msgs[0]["ReceiptHandle"].as_s})[1].should eq "MissingParameter"
    end
  end

  it "shortens the visibility timeout" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("shorten")
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "x"})
      handle = receive(sqs, url)[0]["ReceiptHandle"].as_s
      sqs.json("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: handle, VisibilityTimeout: 1})
      receive(sqs, url, WaitTimeSeconds: 3).size.should eq 1
    end
  end

  it "long polls until a message arrives" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("poll")
      spawn do
        sleep 0.3.seconds
        sqs.json("SendMessage", {QueueUrl: url, MessageBody: "late"})
      end
      started = Time.instant
      msgs = receive(sqs, url, WaitTimeSeconds: 5)
      elapsed = Time.instant - started
      msgs.size.should eq 1
      msgs[0]["Body"].as_s.should eq "late"
      elapsed.should be < 3.seconds
      elapsed.should be >= 0.2.seconds
    end
  end

  it "long polls for the queue default wait time" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("poll-default", {ReceiveMessageWaitTimeSeconds: "1"})
      started = Time.instant
      receive(sqs, url).should be_empty
      (Time.instant - started).should be >= 0.9.seconds
      sqs.error("ReceiveMessage", {QueueUrl: url, WaitTimeSeconds: 21})[1].should eq "InvalidParameterValue"
    end
  end

  it "round trips message attributes with checksums" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("attrs")
      attributes = {
        "color"  => {DataType: "String", StringValue: "red"},
        "count"  => {DataType: "Number", StringValue: "3"},
        "blob"   => {DataType: "Binary", BinaryValue: Base64.strict_encode(Bytes[0, 1])},
        "custom" => {DataType: "String.mytype", StringValue: "x"},
      }
      sent = sqs.json("SendMessage", {QueueUrl: url, MessageBody: "with attrs", MessageAttributes: attributes})
      sent["MD5OfMessageAttributes"].as_s.size.should eq 32

      msg = receive(sqs, url, MessageAttributeNames: ["All"])[0]
      msg["MD5OfMessageAttributes"].as_s.should eq sent["MD5OfMessageAttributes"].as_s
      got = msg["MessageAttributes"]
      got["color"]["StringValue"].as_s.should eq "red"
      got["count"]["DataType"].as_s.should eq "Number"
      got["blob"]["BinaryValue"].as_s.should eq Base64.strict_encode(Bytes[0, 1])
      got["custom"]["DataType"].as_s.should eq "String.mytype"
      got.as_h.keys.should eq %w[blob color count custom] # sorted

      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "filtered", MessageAttributes: attributes})
      filtered = receive(sqs, url, MessageAttributeNames: ["color", "c.*"])[0]
      filtered["MessageAttributes"].as_h.keys.should eq %w[color count custom]
      filtered["MD5OfMessageAttributes"].as_s.should_not eq sent["MD5OfMessageAttributes"].as_s

      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "none", MessageAttributes: attributes})
      plain = receive(sqs, url)[0]
      plain["MessageAttributes"]?.should be_nil
      plain["MD5OfMessageAttributes"]?.should be_nil
    end
  end

  it "validates message bodies and sizes" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("valid", {MaximumMessageSize: "1024"})
      sqs.error("SendMessage", {QueueUrl: url})[1].should eq "MissingParameter"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: ""})[1].should eq "InvalidParameterValue"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "a" * 1025})[1].should eq "InvalidParameterValue"
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "a" * 1024})
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "a" * 1000, MessageAttributes: {"k" => {DataType: "String", StringValue: "v" * 30}}})[1].should eq "InvalidParameterValue"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "bad  char"})[1].should eq "InvalidMessageContents"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x", DelaySeconds: 901})[1].should eq "InvalidParameterValue"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x", MessageAttributes: {"AWS.x" => {DataType: "String", StringValue: "v"}}})[1].should eq "InvalidParameterValue"
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "tabs\tnewlines\n and unicode é \u{1F389} are fine"})
    end
  end

  it "delays messages" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("delayed")
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "later", DelaySeconds: 1})
      receive(sqs, url).should be_empty
      s.vhosts["/"].exchange?("sqs.delayed").should_not be_nil
      msgs = receive(sqs, url, WaitTimeSeconds: 5)
      msgs.size.should eq 1
      msgs[0]["Body"].as_s.should eq "later"
      # queue level delay
      durl = sqs.create_queue("delayed-queue", {DelaySeconds: "1"})
      sqs.json("SendMessage", {QueueUrl: durl, MessageBody: "q-later"})
      receive(sqs, durl).should be_empty
      receive(sqs, durl, WaitTimeSeconds: 5).size.should eq 1
      # explicit 0 overrides the queue delay
      sqs.json("SendMessage", {QueueUrl: durl, MessageBody: "now", DelaySeconds: 0})
      receive(sqs, durl).size.should eq 1
    end
  end

  it "sends and deletes in batches" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("batch")
      result = sqs.json("SendMessageBatch", {QueueUrl: url, Entries: [
        {Id: "a", MessageBody: "one"},
        {Id: "b", MessageBody: "two", MessageAttributes: {"k" => {DataType: "String", StringValue: "v"}}},
        {Id: "c", MessageBody: ""},
      ]})
      result["Successful"].as_a.map(&.["Id"].as_s).should eq %w[a b]
      result["Successful"].as_a[1]["MD5OfMessageAttributes"].as_s.size.should eq 32
      result["Successful"].as_a[0]["MD5OfMessageBody"].as_s.should eq Digest::MD5.hexdigest("one")
      failed = result["Failed"].as_a
      failed.size.should eq 1
      failed[0]["Id"].as_s.should eq "c"
      failed[0]["SenderFault"].as_bool.should be_true
      failed[0]["Code"].as_s.should eq "InvalidParameterValue"

      msgs = receive(sqs, url, MaxNumberOfMessages: 10)
      msgs.size.should eq 2
      del = sqs.json("DeleteMessageBatch", {QueueUrl: url, Entries: [
        {Id: "x", ReceiptHandle: msgs[0]["ReceiptHandle"].as_s},
        {Id: "y", ReceiptHandle: "bogus"},
      ]})
      del["Successful"].as_a.map(&.["Id"].as_s).should eq ["x"]
      del["Failed"].as_a[0]["Code"].as_s.should eq "ReceiptHandleIsInvalid"

      vis = sqs.json("ChangeMessageVisibilityBatch", {QueueUrl: url, Entries: [
        {Id: "v", ReceiptHandle: msgs[1]["ReceiptHandle"].as_s, VisibilityTimeout: 0},
      ]})
      vis["Successful"].as_a.size.should eq 1
      receive(sqs, url).size.should eq 1
    end
  end

  it "validates batch requests" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("batch-validation")
      sqs.error("SendMessageBatch", {QueueUrl: url, Entries: [] of String})[1].should eq "EmptyBatchRequest"
      sqs.error("SendMessageBatch", {QueueUrl: url})[1].should eq "EmptyBatchRequest"
      entries = (1..11).map { |i| {Id: "id#{i}", MessageBody: "x"} }
      sqs.error("SendMessageBatch", {QueueUrl: url, Entries: entries})[1].should eq "TooManyEntriesInBatchRequest"
      dup = [{Id: "same", MessageBody: "x"}, {Id: "same", MessageBody: "y"}]
      sqs.error("SendMessageBatch", {QueueUrl: url, Entries: dup})[1].should eq "BatchEntryIdsNotDistinct"
      sqs.error("SendMessageBatch", {QueueUrl: url, Entries: [{Id: "bad id!", MessageBody: "x"}]})[1].should eq "InvalidBatchEntryId"
      big = [{Id: "a", MessageBody: "x" * 200_000}, {Id: "b", MessageBody: "x" * 200_000}]
      sqs.error("SendMessageBatch", {QueueUrl: url, Entries: big})[1].should eq "BatchRequestTooLong"
    end
  end

  it "supports fifo queues with deduplication" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("orders.fifo", {FifoQueue: "true"})
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x"})[1].should eq "MissingParameter"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x", MessageGroupId: "g"})[1].should eq "InvalidParameterValue"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x", MessageGroupId: "g", MessageDeduplicationId: "d", DelaySeconds: 1})[1].should eq "InvalidParameterValue"
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "first", MessageGroupId: "g", MessageDeduplicationId: "d1"})
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "dup", MessageGroupId: "g", MessageDeduplicationId: "d1"})
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "second", MessageGroupId: "g", MessageDeduplicationId: "d2"})
      msgs = receive(sqs, url, MaxNumberOfMessages: 10, AttributeNames: ["MessageGroupId", "MessageDeduplicationId"])
      msgs.map(&.["Body"].as_s).should eq %w[first second]
      msgs[0]["Attributes"]["MessageGroupId"].as_s.should eq "g"
      msgs[0]["Attributes"]["MessageDeduplicationId"].as_s.should eq "d1"

      curl = sqs.create_queue("content.fifo", {FifoQueue: "true", ContentBasedDeduplication: "true"})
      sqs.json("SendMessage", {QueueUrl: curl, MessageBody: "same", MessageGroupId: "g"})
      sqs.json("SendMessage", {QueueUrl: curl, MessageBody: "same", MessageGroupId: "g"})
      sqs.json("SendMessage", {QueueUrl: curl, MessageBody: "other", MessageGroupId: "g"})
      receive(sqs, curl, MaxNumberOfMessages: 10).map(&.["Body"].as_s).should eq %w[same other]
    end
  end

  it "interoperates with AMQP publishers and consumers" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("interop")
      with_channel(s) do |ch|
        q = ch.queue("interop", passive: true)
        q.publish_confirm "from amqp"
        msgs = receive(sqs, url, MessageSystemAttributeNames: ["All"])
        msgs.size.should eq 1
        msgs[0]["Body"].as_s.should eq "from amqp"
        msgs[0]["MessageId"].as_s.size.should eq 36
        msgs[0]["Attributes"]["SentTimestamp"].as_s.to_i64.should be > 0
        msgs[0]["Attributes"]["SenderId"]?.should be_nil
        # the generated id is stable across redeliveries
        sqs.json("ChangeMessageVisibility", {QueueUrl: url, ReceiptHandle: msgs[0]["ReceiptHandle"].as_s, VisibilityTimeout: 0})
        again = receive(sqs, url)
        again[0]["MessageId"].should eq msgs[0]["MessageId"]
        sqs.json("DeleteMessage", {QueueUrl: url, ReceiptHandle: again[0]["ReceiptHandle"].as_s})

        sqs.json("SendMessage", {QueueUrl: url, MessageBody: "from sqs",
                                 MessageAttributes: {"k" => {DataType: "String", StringValue: "v"}}})
        amqp_msg = q.get(no_ack: true).not_nil!
        amqp_msg.body_io.to_s.should eq "from sqs"
        amqp_msg.properties.content_type.should eq "text/plain; charset=utf-8"
        amqp_msg.properties.delivery_mode.should eq 2
        amqp_msg.properties.message_id.not_nil!.size.should eq 36
        headers = amqp_msg.properties.headers.not_nil!
        headers["x-sqs-sender-id"].should eq "guest"
        headers["x-sqs-attributes"].as(AMQP::Client::Arguments)["k"].as(AMQP::Client::Arguments)["StringValue"].should eq "v"
      end
    end
  end

  it "requeues in-flight messages when the server restarts" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("restart")
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "durable"})
      receive(sqs, url).size.should eq 1
      s.vhosts["/"].queue("restart").unacked_count.should eq 1
      s.restart_stores_for_specs
      s.vhosts["/"].queue("restart").message_count.should eq 1
      s.vhosts["/"].queue("restart").unacked_count.should eq 0
    end
  end
end
