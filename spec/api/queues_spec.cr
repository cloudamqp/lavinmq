require "../spec_helper"

describe AvalancheMQ::QueuesController do
  describe "GET /api/queues" do
    it "should return all queues" do
      s.vhosts["/"].declare_queue("", false, false)
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
      s.vhosts["/"].declare_queue("", false, false)
      response = get("/api/queues/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "GET /api/queues/vhost/name" do
    it "should return queue" do
      s.vhosts["/"].declare_queue("q0", false, false)
      response = get("/api/queues/%2f/q0")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_queue("q0")
    end

    it "should return 404 if queue does not exist" do
      response = get("/api/queues/%2f/404")
      response.status_code.should eq 404
    end

    it "should return message stats" do
      with_channel do |ch|
        q = ch.queue("stats_q")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
      end
      response = get("/api/queues/%2f/stats_q")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["message_stats"]["publish_details"]["rate"].nil?.should be_false
    ensure
      s.vhosts["/"].delete_queue("stats_q")
    end
  end

  describe "PUT /api/queues/vhost/name" do
    it "should create queue" do
      body = %({
        "durable": false,
        "auto_delete": true,
        "arguments": {
          "max-length": 10
        }
      })
      response = put("/api/queues/%2f/putqueue", body: body)
      response.status_code.should eq 204
      response = get("/api/queues/%2f/putqueue")
      response.status_code.should eq 200
    ensure
      s.vhosts["/"].delete_queue("putqueue")
    end

    it "should not require any body" do
      response = put("/api/queues/%2f/okq", body: %({}))
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].delete_queue("okq")
    end

    it "should require durable to be the same when overwriting" do
      body = %({
        "durable": true
      })
      response = put("/api/queues/%2f/q1d", body: body)
      response.status_code.should eq 204
      body = %({
        "durable": false
      })
      response = put("/api/queues/%2f/q1d", body: body)
      response.status_code.should eq 400
    ensure
      s.vhosts["/"].delete_queue("q1d")
    end

    it "should not be possible to declare amq. prefixed queues" do
      response = put("/api/queues/%2f/amq.test", body: %({}))
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
      s.vhosts["/"].declare_queue("delq", false, false)
      response = delete("/api/queues/%2f/delq")
      response.status_code.should eq 204
    ensure
      s.vhosts["/"].declare_queue("delq", false, false)
    end

    it "should not delete queue if it has messasge when query param if-unused is set" do
      with_channel do |ch|
        q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q3/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["payload_bytes", "redelivered", "exchange", "routing_key", "message_count",
                "properties", "payload", "payload_encoding"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        s.vhosts["/"].queues["q3"].message_count.should be > 0
      end
    ensure
      s.vhosts["/"].delete_queue("q3")
    end
  end

  describe "POST /api/queues/vhost/name/get" do
    it "should get messages" do
      with_channel do |ch|
        q = ch.queue("q4")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q4/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        s.vhosts["/"].queues["q4"].empty?.should be_true
      end
    ensure
      s.vhosts["/"].delete_queue("q4")
    end

    it "should handle count > message_count" do
      with_channel do |ch|
        q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        sleep 0.05
        body = %({
          "count": 2,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q5/get", body: body)
        response.status_code.should eq 200
      end
    ensure
      s.vhosts["/"].delete_queue("q5")
    end

    it "should handle empty q" do
      with_channel do |ch|
        ch.queue("q6")
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("/api/queues/%2f/q6/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    ensure
      s.vhosts["/"].delete_queue("q6")
    end

    it "should handle base64 encoding" do
      with_channel do |ch|
        q = ch.queue("q7")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        sleep 0.05
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "base64"
        })
        response = post("/api/queues/%2f/q7/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        Base64.decode_string(body[0]["payload"].as_s).should eq "m1"
      end
    ensure
      s.vhosts["/"].delete_queue("q7")
    end
  end
end
