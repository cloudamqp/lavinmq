require "./spec_helper"

describe "SQS authentication and authorization" do
  it "maps the access key id to a user" do
    with_sqs_server do |sqs, s|
      s.users.create("alice", "secret")
      s.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
      sqs.json("CreateQueue", {QueueName: "q"}, user: "alice")
      status, code, _ = sqs.error("ListQueues", user: "nobody")
      status.should eq 403
      code.should eq "InvalidClientTokenId"
      sqs.error("ListQueues", user: "__direct")[1].should eq "InvalidClientTokenId"
    end
  end

  it "requires credentials" do
    with_sqs_server do |sqs, _|
      headers = HTTP::Headers{"Content-Type" => "application/x-amz-json-1.0", "X-Amz-Target" => "AmazonSQS.ListQueues"}
      response = HTTP::Client.post(sqs.base_url, headers: headers, body: "{}")
      response.status_code.should eq 403
      JSON.parse(response.body)["__type"].as_s.should eq "com.amazonaws.sqs#MissingAuthenticationToken"
    end
  end

  it "accepts presigned credentials in the query string" do
    with_sqs_server do |sqs, _|
      headers = HTTP::Headers{"Content-Type" => "application/x-amz-json-1.0", "X-Amz-Target" => "AmazonSQS.ListQueues"}
      response = HTTP::Client.post("#{sqs.base_url}/?X-Amz-Credential=guest%2F20260917%2Feu-west-1%2Fsqs%2Faws4_request", headers: headers, body: "{}")
      response.status_code.should eq 200
    end
  end

  it "enforces vhost permissions per action" do
    with_sqs_server do |sqs, s|
      s.users.create("reader", "x")
      s.users.add_permission("reader", "/", /^$/, /.*/, /^$/) # read only
      s.users.create("writer", "x")
      s.users.add_permission("writer", "/", /^$/, /^$/, /.*/) # write only
      s.users.create("outsider", "x")
      url = sqs.create_queue("perm")
      sqs.error("CreateQueue", {QueueName: "other"}, user: "reader")[1].should eq "AccessDeniedException"
      sqs.error("SendMessage", {QueueUrl: url, MessageBody: "x"}, user: "reader")[1].should eq "AccessDeniedException"
      sqs.json("SendMessage", {QueueUrl: url, MessageBody: "x"}, user: "writer")
      sqs.error("ReceiveMessage", {QueueUrl: url}, user: "writer")[1].should eq "AccessDeniedException"
      sqs.json("ReceiveMessage", {QueueUrl: url}, user: "reader")["Messages"].as_a.size.should eq 1
      sqs.error("DeleteQueue", {QueueUrl: url}, user: "reader")[1].should eq "AccessDeniedException"
      status, code, _ = sqs.error("ListQueues", user: "outsider")
      status.should eq 403
      code.should eq "AccessDeniedException"
      sqs.json("ListQueues", user: "writer")["QueueUrls"].as_a.should be_empty
      sqs.json("ListQueues", user: "reader")["QueueUrls"].as_a.size.should eq 1
    end
  end

  it "echoes the credential region in ARNs" do
    with_sqs_server do |sqs, _|
      url = sqs.create_queue("regional")
      hdrs = sqs.headers("guest", "eu-north-1")
      hdrs["X-Amz-Target"] = "AmazonSQS.GetQueueAttributes"
      response = HTTP::Client.post(sqs.base_url, headers: hdrs, body: {QueueUrl: url, AttributeNames: ["QueueArn"]}.to_json)
      JSON.parse(response.body)["Attributes"]["QueueArn"].as_s.should eq "arn:aws:sqs:eu-north-1:000000000000:regional"
    end
  end
end

describe "SQS protocol handling" do
  it "answers Query protocol clients with an XML error" do
    with_sqs_server do |sqs, _|
      headers = HTTP::Headers{"Content-Type" => "application/x-www-form-urlencoded; charset=utf-8"}
      response = HTTP::Client.post(sqs.base_url, headers: headers, body: "Action=ListQueues&Version=2012-11-05")
      response.status_code.should eq 400
      response.content_type.should eq "text/xml"
      response.body.should contain "<Code>InvalidAction</Code>"
      response.body.should contain "<RequestId>"
    end
  end

  it "rejects other methods and content types" do
    with_sqs_server do |sqs, _|
      HTTP::Client.get(sqs.base_url).status_code.should eq 400
      headers = HTTP::Headers{"Content-Type" => "application/json", "X-Amz-Target" => "AmazonSQS.ListQueues"}
      HTTP::Client.post(sqs.base_url, headers: headers, body: "{}").status_code.should eq 400
    end
  end

  it "rejects malformed json" do
    with_sqs_server do |sqs, _|
      hdrs = sqs.headers("guest")
      hdrs["X-Amz-Target"] = "AmazonSQS.ListQueues"
      response = HTTP::Client.post(sqs.base_url, headers: hdrs, body: "{not json")
      response.status_code.should eq 400
      JSON.parse(response.body)["__type"].as_s.should eq "com.amazonaws.sqs#SerializationException"
    end
  end

  it "treats an empty body as no parameters" do
    with_sqs_server do |sqs, _|
      hdrs = sqs.headers("guest")
      hdrs["X-Amz-Target"] = "AmazonSQS.ListQueues"
      HTTP::Client.post(sqs.base_url, headers: hdrs, body: "").status_code.should eq 200
    end
  end
end
