require "../spec_helper"

describe LavinMQ::HTTP::ExchangesController do
  describe "GET /api/exchanges" do
    it "should return all exchanges" do
      response = get("/api/exchanges")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["arguments", "internal", "auto_delete", "durable", "type", "vhost", "name"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end
  end

  describe "GET /api/exchanges/vhost" do
    it "should return all exchanges for a vhost" do
      response = get("/api/exchanges/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "GET /api/exchanges/vhost/name" do
    it "should return exchange" do
      response = get("/api/exchanges/%2f/amq.topic")
      response.status_code.should eq 200
    end

    it "should return 404 if exchange does not exist" do
      response = get("/api/exchanges/%2f/404")
      response.status_code.should eq 404
    end
  end

  describe "PUT /api/exchanges/vhost/name" do
    it "should create exchange" do
      body = %({
        "type": "topic",
        "durable": false,
        "internal": false,
        "auto_delete": true,
        "arguments": {
          "alternate-exchange": "spexchange"
        }
      })
      response = put("/api/exchanges/%2f/spechange", body: body)
      response.status_code.should eq 201
      response = get("/api/exchanges/%2f/spechange")
      response.status_code.should eq 200
    end

    it "should require type" do
      response = put("/api/exchanges/%2f/faulty", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field 'type' is required/)
    end

    it "should require a known type" do
      body = %({ "type": "tut" })
      response = put("/api/exchanges/%2f/faulty", body: body)
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/invalid exchange type/)
    end

    it "should require arguments if the type demands it" do
      body = %({ "type": "x-delayed-message" })
      response = put("/api/exchanges/%2f/faulty", body: body)
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Missing required argument/)
    end

    it "should handle unexpected input" do
      response = put("/api/exchanges/%2f/faulty", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Input needs to be a JSON object.")
    end

    it "should handle invalid JSON" do
      response = put("/api/exchanges/%2f/faulty", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end

    it "should require durable to be the same when overwriting" do
      body = %({
        "type": "topic",
        "durable": true,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      response = put("/api/exchanges/%2f/spechange", body: body)
      response.status_code.should eq 201
      body = %({
        "type": "topic",
        "durable": false,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      response = put("/api/exchanges/%2f/spechange", body: body)
      response.status_code.should eq 400
    end

    it "should not be possible to declare amq. prefixed exchanges" do
      body = %({
        "type": "topic"
      })
      response = put("/api/exchanges/%2f/amq.test", body: body)
      response.status_code.should eq 400
    end

    it "should redeclare identical delayed_message_exchange" do
      body = %({
        "type": "x-delayed-message",
        "durable": true,
        "internal": false,
        "auto_delete": false,
        "arguments": {
          "x-delayed-type": "fanout",
          "test": "hello"
        }
      })
      response = put("/api/exchanges/%2f/spechange", body: body)
      response.status_code.should eq 201
      response = put("/api/exchanges/%2f/spechange", body: body)
      response.status_code.should eq 204
    end

    it "should require config access to declare" do
      body = %({
        "type": "topic"
      })
      hdrs = HTTP::Headers{"Authorization" => "Basic dGVzdF9wZXJtOnB3"}
      response = put("/api/exchanges/%2f/test_perm", headers: hdrs, body: body)
      response.status_code.should eq 401
    end
  end

  describe "DELETE /api/exchanges/vhost/name" do
    it "should delete exchange" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      response = delete("/api/exchanges/%2f/spechange")
      response.status_code.should eq 204
    end

    it "should not delete exchange if in use as source when query param if-unused is set" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      Server.vhosts["/"].declare_queue("ex_q1", false, false)
      Server.vhosts["/"].bind_queue("ex_q1", "spechange", ".*")
      response = delete("/api/exchanges/%2f/spechange?if-unused=true")
      response.status_code.should eq 400
    end

    it "should not delete exchange if in use as destination when query param if-unused is set" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      Server.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
      Server.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
      response = delete("/api/exchanges/%2f/spechange?if-unused=true")
      response.status_code.should eq 400
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/source" do
    it "should list bindings" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      Server.vhosts["/"].declare_queue("ex_q1", false, false)
      Server.vhosts["/"].bind_queue("ex_q1", "spechange", ".*")
      response = get("/api/exchanges/%2f/spechange/bindings/source")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.size.should eq 1
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/destination" do
    it "should list bindings" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      Server.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
      Server.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
      response = get("/api/exchanges/%2f/spechange/bindings/destination")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.size.should eq 1
    end
  end

  describe "POST /api/exchanges/vhost/name/publish" do
    it "should publish" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      Server.vhosts["/"].declare_queue("q1p", false, false)
      Server.vhosts["/"].bind_queue("q1p", "spechange", "*")
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
      response = post("/api/exchanges/%2f/spechange/publish", body: body)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["routed"].as_bool.should be_true
      Server.vhosts["/"].queues["q1p"].message_count.should eq 1
    end

    it "should require all args" do
      Server.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      response = post("/api/exchanges/%2f/spechange/publish", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Fields .+ are required/)
    end

    it "should handle string encoding" do
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
      with_channel do |ch|
        q = ch.queue("q2", durable: false)
        x = ch.exchange("str_enc", "topic", passive: false)
        q.bind(x.name, "*")
        response = post("/api/exchanges/%2f/str_enc/publish", body: body)
        response.status_code.should eq 200
        msgs = [] of AMQP::Client::DeliverMessage
        q.subscribe { |msg| msgs << msg }
        wait_for { msgs.size == 1 }
        msgs.first.not_nil!.body_io.to_s.should eq("test")
      end
    end

    it "should handle base64 encoding" do
      payload = Base64.urlsafe_encode("test")
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "#{payload}",
        "payload_encoding": "base64"
      })
      with_channel do |ch|
        q = ch.queue("q2", durable: false)
        x = ch.exchange("str_enc", "topic", passive: false)
        q.bind(x.name, "*")
        response = post("/api/exchanges/%2f/str_enc/publish", body: body)
        response.status_code.should eq 200
        msgs = [] of AMQP::Client::DeliverMessage
        q.subscribe { |msg| msgs << msg }
        wait_for { msgs.size == 1 }
        msgs.first.not_nil!.body_io.to_s.should eq("test")
      end
    end
  end
end
