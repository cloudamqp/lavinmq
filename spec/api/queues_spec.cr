require "../spec_helper"

describe AvalancheMQ::QueuesController do
  describe "GET /api/queues" do
    it "should return all queues" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].declare_queue("q0", false, false)
      response = get("http://localhost:8080/api/queues")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "durable" , "exclusive", "auto_delete" , "arguments", "consumers" , "vhost",
       "messages", "ready", "unacked", "policy", "exclusive_consumer_tag", "state",
       "effective_policy_definition"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end
  end

  describe "GET /api/queues/vhost" do
    it "should return all queues for a vhost" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].declare_queue("q0", false, false)
      response = get("http://localhost:8080/api/queues/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end

  describe "GET /api/queues/vhost/name" do
    it "should return queue" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].declare_queue("q0", false, false)
      response = get("http://localhost:8080/api/queues/%2f/q0")
      response.status_code.should eq 200
    ensure
      close(h)
    end

    it "should return 404 if queue does not exist" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/queues/%2f/404")
      response.status_code.should eq 404
    ensure
      close(h)
    end
  end

  describe "PUT /api/queues/vhost/name" do
    it "should create queue" do
      s, h = create_servers
      listen(h)
      body = %({
        "durable": false,
        "auto_delete": true,
        "arguments": {
          "max-length": 10
        }
      })
      s.vhosts["/"].delete_queue("putqueue")
      response = put("http://localhost:8080/api/queues/%2f/putqueue", body: body)
      response.status_code.should eq 201
      response = get("http://localhost:8080/api/queues/%2f/putqueue")
      response.status_code.should eq 200
    ensure
      close(h)
    end

    it "should not require any body" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].delete_queue("okq")
      response = put("http://localhost:8080/api/queues/%2f/okq", body: %({}))
      response.status_code.should eq 201
    ensure
      close(h)
    end

    it "should require durable to be the same when overwriting" do
      s, h = create_servers
      listen(h)
      body = %({
        "durable": true
      })
      s.vhosts["/"].delete_queue("q1")
      response = put("http://localhost:8080/api/queues/%2f/q1", body: body)
      response.status_code.should eq 201
      body = %({
        "durable": false
      })
      response = put("http://localhost:8080/api/queues/%2f/q1", body: body)
      response.status_code.should eq 400
    ensure
      close(h)
    end

    it "should not be possible to declare amq. prefixed queues" do
      s, h = create_servers
      listen(h)
      response = put("http://localhost:8080/api/queues/%2f/amq.test", body: %({}))
      response.status_code.should eq 400
    ensure
      close(h)
    end

    it "should require config access to declare" do
      s, h = create_servers
      s.users.create("test_perm", "pw")
      s.users.add_permission("test_perm", "/", /^$/, /^$/, /^$/)
      listen(h)
      hdrs = HTTP::Headers{"Authorization" => "Basic dGVzdF9wZXJtOnB3"}
      response = put("http://localhost:8080/api/queues/%2f/test_perm", headers: hdrs, body: %({}))
      response.status_code.should eq 401
    ensure
      close(h)
    end
  end

  describe "DELETE /api/queues/vhost/name" do
    it "should delete queue" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].declare_queue("delq", false, false)
      response = delete("http://localhost:8080/api/queues/%2f/delq")
      response.status_code.should eq 204
    ensure
      close(h)
    end

    it "should not delete queue if it has messasge when query param if-unused is set" do
      s, h = create_servers
      spawn { h.try &.listen }
      spawn { s.try &.listen(5672) }
      s.not_nil!.vhosts["/"].delete_queue("q1")
      Fiber.yield
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q1")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        response = delete("http://localhost:8080/api/queues/%2f/q1?if-unused=true")
        response.status_code.should eq 400
      end
    ensure
      close(h, s)
    end

    it "should not delete queue if it has consumers when query param if-unused is set" do
      s, h = create_servers
      spawn { h.try &.listen }
      spawn { s.try &.listen(5672) }
      Fiber.yield
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q2")
        q.subscribe do |m|
          response = delete("http://localhost:8080/api/queues/%2f/q2?if-unused=true")
          response.status_code.should eq 400
        end
      end
    ensure
      close(h, s)
    end
  end

  describe "POST /api/queues/vhost/name/get" do
    it "should peek messages" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("http://localhost:8080/api/queues/%2f/q3/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["payload_bytes", "redelivered", "exchange", "routing_key", "message_count",
                "properties", "payload", "payload_encoding"]
        body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        s.not_nil!.vhosts["/"].queues["q3"].message_count.should eq 1
      end
    ensure
      close(h, s)
    end

    it "should get messages" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q4")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        body = %({
          "count": 1,
          "ack_mode": "get",
          "encoding": "auto"
        })
        response = post("http://localhost:8080/api/queues/%2f/q4/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        s.not_nil!.vhosts["/"].queues["q4"].empty?.should be_true
      end
    ensure
      close(h, s)
    end

    it "should handle count > message_count" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        body = %({
          "count": 2,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("http://localhost:8080/api/queues/%2f/q5/get", body: body)
        response.status_code.should eq 200
      end
    ensure
      close(h, s)
    end

    it "should handle empty q" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q6")
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "auto"
        })
        response = post("http://localhost:8080/api/queues/%2f/q6/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    ensure
      close(h, s)
    end

    it "should handle base64 encoding" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q7")
        x = ch.exchange("", "direct")
        x.publish AMQP::Message.new("m1"), q.name
        body = %({
          "count": 1,
          "ack_mode": "peek",
          "encoding": "base64"
        })
        response = post("http://localhost:8080/api/queues/%2f/q7/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        Base64.decode_string(body[0]["payload"].as_s).should eq "m1"
      end
    ensure
      close(h)
      s.try &.close
    end
  end
end
