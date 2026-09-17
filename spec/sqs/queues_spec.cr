require "./spec_helper"

describe "SQS queue management" do
  it "creates a queue and returns its url" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("orders")
      url.should eq "#{sqs.base_url}/000000000000/orders"
      q = s.vhosts["/"].queue("orders")
      q.durable?.should be_true
      q.arguments["x-message-ttl"].should eq 345_600_000
    end
  end

  it "is idempotent for identical attributes and rejects different ones" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("orders", {VisibilityTimeout: "60"})
      sqs.create_queue("orders", {VisibilityTimeout: "60"}).should eq url
      sqs.create_queue("orders").should eq url
      status, code, _ = sqs.error("CreateQueue", {QueueName: "orders", Attributes: {VisibilityTimeout: "61"}})
      status.should eq 400
      code.should eq "QueueNameExists"
    end
  end

  it "validates queue names and attributes" do
    with_sqs_server do |sqs, _|
      sqs.error("CreateQueue", {QueueName: "bad name"})[1].should eq "InvalidParameterValue"
      sqs.error("CreateQueue", {QueueName: "a" * 81})[1].should eq "InvalidParameterValue"
      sqs.error("CreateQueue", {QueueName: "q", Attributes: {Nope: "1"}})[1].should eq "InvalidAttributeName"
      sqs.error("CreateQueue", {QueueName: "q", Attributes: {VisibilityTimeout: "99999"}})[1].should eq "InvalidAttributeValue"
      sqs.error("CreateQueue", {QueueName: "q", Attributes: {FifoQueue: "maybe"}})[1].should eq "InvalidAttributeValue"
      sqs.error("CreateQueue", {QueueName: "q.fifo"})[1].should eq "InvalidParameterValue"
      sqs.error("CreateQueue", {QueueName: "q", Attributes: {FifoQueue: "true"}})[1].should eq "InvalidParameterValue"
      sqs.error("CreateQueue", NamedTuple.new)[1].should eq "MissingParameter"
    end
  end

  it "adopts queues declared over AMQP" do
    with_sqs_server do |sqs, s|
      s.vhosts["/"].declare_queue("amqp-q", true, false)
      sqs.json("GetQueueUrl", {QueueName: "amqp-q"})["QueueUrl"].as_s.should end_with "/000000000000/amqp-q"
      sqs.create_queue("amqp-q", {VisibilityTimeout: "5"}).should end_with "/amqp-q"
      attrs = sqs.json("GetQueueAttributes", {QueueUrl: sqs.create_queue("amqp-q"), AttributeNames: ["VisibilityTimeout"]})
      attrs["Attributes"]["VisibilityTimeout"].as_s.should eq "5"
    end
  end

  it "returns QueueDoesNotExist with the legacy query code" do
    with_sqs_server do |sqs, _|
      response = sqs.call("GetQueueUrl", {QueueName: "nope"})
      response.status_code.should eq 400
      response.headers["x-amzn-query-error"].should eq "AWS.SimpleQueueService.NonExistentQueue;Sender"
      response.headers["x-amzn-RequestId"].should_not be_empty
      body = JSON.parse(response.body)
      body["__type"].as_s.should eq "com.amazonaws.sqs#QueueDoesNotExist"
      body["message"].as_s.should eq "The specified queue does not exist."
      sqs.error("DeleteQueue", {QueueUrl: "#{sqs.base_url}/000000000000/nope"})[1].should eq "QueueDoesNotExist"
      sqs.error("DeleteQueue", {QueueUrl: "#{sqs.base_url}/nonexistent-vhost/q"})[1].should eq "QueueDoesNotExist"
      sqs.error("DeleteQueue", {QueueUrl: "garbage"})[0].should eq 404
    end
  end

  it "lists queues with prefix and pagination" do
    with_sqs_server do |sqs, s|
      %w[a1 a2 a3 b1].each { |n| sqs.create_queue(n) }
      s.vhosts["/"].declare_queue("a4-amqp", true, false)
      all = sqs.json("ListQueues")["QueueUrls"].as_a.map(&.as_s)
      all.map(&.split('/').last).should eq %w[a1 a2 a3 a4-amqp b1]
      prefixed = sqs.json("ListQueues", {QueueNamePrefix: "a"})
      prefixed["QueueUrls"].as_a.size.should eq 4
      prefixed["NextToken"]?.should be_nil
      page1 = sqs.json("ListQueues", {MaxResults: 2})
      page1["QueueUrls"].as_a.map(&.as_s).map(&.split('/').last).should eq %w[a1 a2]
      page2 = sqs.json("ListQueues", {MaxResults: 2, NextToken: page1["NextToken"].as_s})
      page2["QueueUrls"].as_a.map(&.as_s).map(&.split('/').last).should eq %w[a3 a4-amqp]
      page3 = sqs.json("ListQueues", {MaxResults: 2, NextToken: page2["NextToken"].as_s})
      page3["QueueUrls"].as_a.size.should eq 1
      page3["NextToken"]?.should be_nil
    end
  end

  it "deletes queues" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("tmp")
      sqs.json("DeleteQueue", {QueueUrl: url})
      s.vhosts["/"].queue?("tmp").should be_nil
      sqs.error("GetQueueUrl", {QueueName: "tmp"})[1].should eq "QueueDoesNotExist"
    end
  end

  it "reports attributes, including live counts" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("attrs", {DelaySeconds: "0", MessageRetentionPeriod: "120"})
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "one"})
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "two"})
      sqs.json("ReceiveMessage", {QueueUrl: url})
      attrs = sqs.json("GetQueueAttributes", {QueueUrl: url, AttributeNames: ["All"]})["Attributes"]
      attrs["ApproximateNumberOfMessages"].as_s.should eq "1"
      attrs["ApproximateNumberOfMessagesNotVisible"].as_s.should eq "1"
      attrs["MessageRetentionPeriod"].as_s.should eq "120"
      attrs["VisibilityTimeout"].as_s.should eq "30"
      attrs["QueueArn"].as_s.should eq "arn:aws:sqs:us-east-1:000000000000:attrs"
      attrs["CreatedTimestamp"].as_s.to_i64.should be_close(Time.utc.to_unix, 5)
      subset = sqs.json("GetQueueAttributes", {QueueUrl: url, AttributeNames: ["QueueArn", "DelaySeconds"]})["Attributes"]
      subset.as_h.keys.sort!.should eq %w[DelaySeconds QueueArn]
      sqs.json("GetQueueAttributes", {QueueUrl: url})["Attributes"].as_h.size.should be > 5
      sqs.error("GetQueueAttributes", {QueueUrl: url, AttributeNames: ["Bogus"]})[1].should eq "InvalidAttributeName"
    end
  end

  it "sets attributes" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("set")
      sqs.json("SetQueueAttributes", {QueueUrl: url, Attributes: {VisibilityTimeout: "2", ReceiveMessageWaitTimeSeconds: "1"}})
      attrs = sqs.json("GetQueueAttributes", {QueueUrl: url, AttributeNames: ["VisibilityTimeout", "ReceiveMessageWaitTimeSeconds"]})["Attributes"]
      attrs["VisibilityTimeout"].as_s.should eq "2"
      attrs["ReceiveMessageWaitTimeSeconds"].as_s.should eq "1"
      sqs.error("SetQueueAttributes", {QueueUrl: url, Attributes: {FifoQueue: "true"}})[1].should eq "InvalidAttributeName"
      sqs.error("SetQueueAttributes", {QueueUrl: url, Attributes: {RedrivePolicy: "not json"}})[1].should eq "InvalidAttributeValue"
      sqs.error("SetQueueAttributes", {QueueUrl: url})[1].should eq "MissingParameter"
    end
  end

  it "purges a queue at most once a minute" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("purge")
      3.times { sqs.json("SendMessage", {QueueUrl: url, MessageBody: "x"}) }
      sqs.json("PurgeQueue", {QueueUrl: url})
      s.vhosts["/"].queue("purge").message_count.should eq 0
      status, code, _ = sqs.error("PurgeQueue", {QueueUrl: url})
      status.should eq 403
      code.should eq "PurgeQueueInProgress"
    end
  end

  it "tags queues and persists the metadata" do
    with_sqs_server do |sqs, s|
      url = sqs.create_queue("tagged")
      sqs.json("TagQueue", {QueueUrl: url, Tags: {env: "test", team: "mq"}})
      sqs.json("ListQueueTags", {QueueUrl: url})["Tags"].as_h.transform_values(&.as_s).should eq({"env" => "test", "team" => "mq"})
      sqs.json("UntagQueue", {QueueUrl: url, TagKeys: ["env"]})
      sqs.json("ListQueueTags", {QueueUrl: url})["Tags"].as_h.keys.should eq ["team"]
      File.exists?(File.join(s.vhosts["/"].data_dir, "sqs_queues.json")).should be_true
      store = LavinMQ::SQS::QueueMetaStore.new(s.vhosts["/"].data_dir, nil)
      store["tagged"]?.try(&.tags).should eq({"team" => "mq"})
    end
  end

  it "uses the request path and QueueOwnerAWSAccountId to select the vhost" do
    with_sqs_server do |sqs, s|
      s.vhosts.create("tenant")
      s.users.add_permission("guest", "tenant", /.*/, /.*/, /.*/)
      url = sqs.json("CreateQueue", {QueueName: "q"}, path: "/tenant")["QueueUrl"].as_s
      url.should eq "#{sqs.base_url}/tenant/q"
      s.vhosts["tenant"].queue?("q").should_not be_nil
      s.vhosts["/"].queue?("q").should be_nil
      sqs.json("GetQueueUrl", {QueueName: "q", QueueOwnerAWSAccountId: "tenant"})["QueueUrl"].as_s.should eq url
      sqs.json("ListQueues", path: "/tenant")["QueueUrls"].as_a.map(&.as_s).should eq [url]
      sqs.json("ListQueues")["QueueUrls"].as_a.should be_empty
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "hi"})
      s.vhosts["tenant"].queue("q").message_count.should eq 1
    end
  end

  it "honours sqs public_url when minting queue urls" do
    with_sqs_server do |sqs, s|
      s.@config.sqs_public_url = "https://sqs.example.com/"
      begin
        sqs.create_queue("pub").should eq "https://sqs.example.com/000000000000/pub"
      ensure
        s.@config.sqs_public_url = ""
      end
    end
  end

  it "reports unsupported and unknown actions" do
    with_sqs_server do |sqs, _|
      sqs.error("StartMessageMoveTask", {SourceArn: "x"})[1].should eq "UnsupportedOperation"
      status, code, _ = sqs.error("Frobnicate")
      status.should eq 400
      code.should eq "InvalidAction"
    end
  end
end
