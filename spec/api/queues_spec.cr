require "../spec_helper"
require "compress/deflate"

describe LavinMQ::HTTP::QueuesController do
  describe "GET /api/queues" do
    it "should return all queues" do
      Server.vhosts["/"].declare_queue("q0", false, false)
      response = get("/api/queues")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "durable", "exclusive", "auto_delete", "arguments", "consumers", "vhost",
              "messages", "ready", "unacked", "policy", "exclusive_consumer_tag", "state",
              "effective_policy_definition"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end
  end

  describe "GET /api/queues/vhost" do
    it "should return all queues for a vhost" do
      Server.vhosts["/"].declare_queue("q0", false, false)
      response = get("/api/queues/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "GET /api/queues/vhost/name" do
    it "should return queue" do
      Server.vhosts["/"].declare_queue("q0", false, false)
      response = get("/api/queues/%2f/q0")
      response.status_code.should eq 200
    end

    it "should return 404 if queue does not exist" do
      response = get("/api/queues/%2f/404")
      response.status_code.should eq 404
    end

    it "should return message stats" do
      with_channel do |ch|
        q = ch.queue("stats_q")
        q.publish "m1"
      end
      response = get("/api/queues/%2f/stats_q")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["message_stats"]["publish_details"]["rate"].nil?.should be_false
    end

    it "should return no persistent message count" do
      with_channel do |ch|
        q = ch.queue("stats_q", auto_delete: false, durable: false, exclusive: false)
        q.publish "m1"
      end
      response = get("/api/queues/%2f/stats_q")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["messages"].should eq 1
      body["messages_persistent"].should eq 0
    end

    it "should return persistent message count" do
      with_channel do |ch|
        q = ch.queue("stats_q", auto_delete: false, durable: true, exclusive: false)
        q.publish "m1"
      end
      response = get("/api/queues/%2f/stats_q")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["messages"].should eq 1
      body["messages_persistent"].should eq 1
    end
  end

  describe "GET /api/queues/vhost/name/size-details" do
    it "should return message size details stats" do
      with_channel do |ch|
        q = ch.queue("stats_q")
        q.publish "m1"
      end
      response = get("/api/queues/%2f/stats_q/size-details")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["ready_max_bytes"].nil?.should be_false
      body["ready_min_bytes"].nil?.should be_false
      body["unacked_max_bytes"].nil?.should be_false
      body["unacked_min_bytes"].nil?.should be_false
    end
  end

  describe "GET /api/queues/vhost/name/bindings" do
    it "should return queue bindings" do
      Server.vhosts["/"].declare_queue("q0", false, false)
      Server.vhosts["/"].bind_queue("q0", "amq.direct", "foo")
      response = get("/api/queues/%2f/q0/bindings?page=1&page_size=100")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["items"].as_a.size.should eq 2
    end
  end

  describe "PUT /api/queues/vhost/name" do
    it "should create queue" do
      body = {
        "durable"     => false,
        "auto_delete" => true,
        "arguments"   => {
          "max-length" => 10,
        },
      }
      response = put("/api/queues/%2f/putqueue", body: body.to_json)
      response.status_code.should eq 201
      response = get("/api/queues/%2f/putqueue")
      response.status_code.should eq 200
      json = JSON.parse(response.body)
      json["name"].should eq "putqueue"
      body.each do |key, value|
        json[key].should eq value
      end
    end

    it "should not require any body" do
      response = put("/api/queues/%2f/okq")
      response.status_code.should eq 201
    end

    it "should require durable to be the same when overwriting" do
      body = %({
        "durable": true
      })
      response = put("/api/queues/%2f/q1d", body: body)
      response.status_code.should eq 201
      body = %({
        "durable": false
      })
      response = put("/api/queues/%2f/q1d", body: body)
      response.status_code.should eq 400
    end

    it "should not be possible to declare amq. prefixed queues" do
      response = put("/api/queues/%2f/amq.test", body: %({}))
      response.status_code.should eq 400
    end

    it "should respond with 400 for PreconditionFailed errors" do
      # supplying 'x-dead-letter-routing-key' is only valid if
      # 'x-dead-letter-exchange' is also in the request
      # so this request generates a Error::PreconditionFailed
      body = %({
        "arguments": {"x-dead-letter-routing-key": "value"}
      })
      response = put("/api/queues/%2f/precond-failed", body: body)
      response.status_code.should eq 400
    end

    it "should require config access to declare" do
      hdrs = HTTP::Headers{"Authorization" => "Basic dGVzdF9wZXJtOnB3"}
      response = put("/api/queues/%2f/test_perm", headers: hdrs, body: %({}))
      response.status_code.should eq 401
    end
  end

  describe "DELETE /api/queues/vhost/name" do
    it "should delete queue" do
      Server.vhosts["/"].declare_queue("delq", false, false)
      response = delete("/api/queues/%2f/delq")
      response.status_code.should eq 204
    end

    it "should not delete queue if it has messasge when query param if-unused is set" do
      with_channel do |ch|
        q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
        q.publish "m1"
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "reject_requeue_true",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q3/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["payload_bytes", "redelivered", "exchange", "routing_key", "message_count",
                "properties", "payload", "payload_encoding"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        Server.vhosts["/"].queues["q3"].message_count.should be > 0
      end
    end
  end

  describe "POST /api/queues/vhost/name/get" do
    it "should get plain text messages" do
      with_channel do |ch|
        q = ch.queue("q4")
        q4 = Server.vhosts["/"].queues["q4"]
        q.publish "m1"
        wait_for { q4.message_count == 1 }
        body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
        response = post("/api/queues/%2f/q4/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body[0]["payload"].should eq "m1"
        q4.empty?.should be_true
      end
    end

    it "should get encoded messages" do
      with_channel do |ch|
        q = ch.queue("q4")
        q4 = Server.vhosts["/"].queues["q4"]
        mem_io = IO::Memory.new
        Compress::Deflate::Writer.open(mem_io, Compress::Deflate::BEST_SPEED) { |deflate| deflate.print("m1") }
        encoded_msg = mem_io.to_s
        q.publish encoded_msg, props: AMQP::Client::Properties.new(content_encoding: "deflate")
        wait_for { q4.message_count == 1 }
        body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
        response = post("/api/queues/%2f/q4/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body[0]["payload"].should eq Base64.urlsafe_encode(encoded_msg)
        body[0]["payload_encoding"].should eq "base64"
        q4.empty?.should be_true
      end
    end

    it "should handle count > message_count" do
      with_channel do |ch|
        q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
        q.publish "m1"
        sleep 0.05
        body = %({
          "count": 2,
          "ack_mode": "get",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q5/get", body: body)
        response.status_code.should eq 200
      end
    end

    it "should handle empty q" do
      with_channel do |ch|
        ch.queue("q6")
        body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q6/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.should be_empty
      end
    end

    it "should handle base64 encoding" do
      with_channel do |ch|
        q = ch.queue("q7")
        q.publish "m1"
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "base64"
        })
        response = post("/api/queues/%2f/q7/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        Base64.decode_string(body[0]["payload"].as_s).should eq "m1"
      end
    end

    it "should not allow get and requeue" do
      with_channel do |ch|
        q = ch.queue("q8")
        q.publish "m1"
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "get",
          "requeue": true,
          "encoding": "base64"
        })
        response = post("/api/queues/%2f/q8/get", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].should eq "Cannot requeue message on get"
      end
    end
  end

  describe "PUT /api/queues/vhost/name/pause" do
    it "should pause the queue" do
      with_channel do |ch|
        ch.queue("confqueue")
        response = put("/api/queues/%2f/confqueue/pause")
        response.status_code.should eq 204
        response = get("/api/queues/%2f/confqueue")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["state"].should eq "paused"
      end
    end
  end

  describe "PUT /api/queues/vhost/name/resume" do
    it "should resume the queue" do
      with_channel do |ch|
        ch.queue("confqueue")

        q = Server.vhosts["/"].queues["confqueue"]
        q.pause!

        response = get("/api/queues/%2f/confqueue")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["state"].should eq "paused"

        response = put("/api/queues/%2f/confqueue/resume")
        response.status_code.should eq 204

        response = get("/api/queues/%2f/confqueue")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["state"].should eq "running"
      end
    end
  end
end
