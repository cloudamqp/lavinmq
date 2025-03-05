require "../spec_helper"

describe LavinMQ::HTTP::ExchangesController do
  describe "GET /api/exchanges" do
    it "should return all exchanges" do
      with_http_server do |http, _|
        response = http.get("/api/exchanges")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["arguments", "internal", "auto_delete", "durable", "type", "vhost", "name"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end

  describe "GET /api/exchanges/vhost" do
    it "should return all exchanges for a vhost" do
      with_http_server do |http, _|
        response = http.get("/api/exchanges/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end

  describe "GET /api/exchanges/vhost/name" do
    it "should return exchange" do
      with_http_server do |http, _|
        response = http.get("/api/exchanges/%2f/amq.topic")
        response.status_code.should eq 200
      end
    end

    it "should return 404 if exchange does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/exchanges/%2f/404")
        response.status_code.should eq 404
      end
    end
  end

  describe "PUT /api/exchanges/vhost/name" do
    it "should create exchange" do
      with_http_server do |http, _|
        body = %({
        "type": "topic",
        "durable": false,
        "internal": false,
        "auto_delete": true,
        "arguments": {
          "alternate-exchange": "spexchange"
        }
      })
        response = http.put("/api/exchanges/%2f/spechange", body: body)
        response.status_code.should eq 201
        response = http.get("/api/exchanges/%2f/spechange")
        response.status_code.should eq 200
      end
    end

    it "should require type" do
      with_http_server do |http, _|
        response = http.put("/api/exchanges/%2f/faulty", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field 'type' is required/)
      end
    end

    it "should require a known type" do
      with_http_server do |http, _|
        body = %({ "type": "tut" })
        response = http.put("/api/exchanges/%2f/faulty", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/invalid exchange type/)
      end
    end

    it "should require arguments if the type demands it" do
      with_http_server do |http, _|
        body = %({ "type": "x-delayed-message" })
        response = http.put("/api/exchanges/%2f/faulty", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Missing required argument/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/exchanges/%2f/faulty", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/exchanges/%2f/faulty", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end

    it "should require durable to be the same when overwriting" do
      with_http_server do |http, _|
        body = %({
        "type": "topic",
        "durable": true,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
        response = http.put("/api/exchanges/%2f/spechange", body: body)
        response.status_code.should eq 201
        body = %({
        "type": "topic",
        "durable": false,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
        response = http.put("/api/exchanges/%2f/spechange", body: body)
        response.status_code.should eq 400
      end
    end

    it "should not be possible to declare amq. prefixed exchanges" do
      with_http_server do |http, _|
        body = %({
        "type": "topic"
      })
        response = http.put("/api/exchanges/%2f/amq.test", body: body)
        response.status_code.should eq 400
      end
    end

    it "should redeclare identical delayed_message_exchange" do
      with_http_server do |http, _|
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
        response = http.put("/api/exchanges/%2f/spechange", body: body)
        response.status_code.should eq 201
        response = http.put("/api/exchanges/%2f/spechange", body: body)
        response.status_code.should eq 204
      end
    end

    it "should require config access to declare" do
      with_http_server do |http, _|
        body = %({
        "type": "topic"
      })
        hdrs = HTTP::Headers{"Authorization" => "Basic dGVzdF9wZXJtOnB3"}
        response = http.put("/api/exchanges/%2f/test_perm", headers: hdrs, body: body)
        response.status_code.should eq 401
      end
    end
  end

  describe "DELETE /api/exchanges/vhost/name" do
    it "should delete exchange" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        response = http.delete("/api/exchanges/%2f/spechange")
        response.status_code.should eq 204
      end
    end

    it "should not delete exchange if in use as source when query param if-unused is set" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        s.vhosts["/"].declare_queue("ex_q1", false, false)
        s.vhosts["/"].bind_queue("ex_q1", "spechange", ".*")
        response = http.delete("/api/exchanges/%2f/spechange?if-unused=true")
        response.status_code.should eq 400
      end
    end

    it "should not delete exchange if in use as destination when query param if-unused is set" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        s.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
        s.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
        response = http.delete("/api/exchanges/%2f/spechange?if-unused=true")
        response.status_code.should eq 400
      end
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/source" do
    it "should list bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        s.vhosts["/"].declare_queue("ex_q1", false, false)
        s.vhosts["/"].bind_queue("ex_q1", "spechange", ".*")
        response = http.get("/api/exchanges/%2f/spechange/bindings/source")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/destination" do
    it "should list bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        s.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
        s.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
        response = http.get("/api/exchanges/%2f/spechange/bindings/destination")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    end
  end

  describe "POST /api/exchanges/vhost/name/publish" do
    it "should publish" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        s.vhosts["/"].declare_queue("q1p", false, false)
        s.vhosts["/"].bind_queue("q1p", "spechange", "*")
        body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
        response = http.post("/api/exchanges/%2f/spechange/publish", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["routed"].as_bool.should be_true
        s.vhosts["/"].queues["q1p"].message_count.should eq 1
      end
    end

    it "should require expiration to be a number" do
      with_http_server do |http, _|
        body = %({
        "properties": { "expiration": "foo" },
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
        response = http.post("/api/exchanges/%2f/amq.direct/publish", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq "Expiration not a number"
      end
    end

    it "should require expiration to be a non-negative number" do
      with_http_server do |http, _|
        body = %({
        "properties": { "expiration": -100 },
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
        response = http.post("/api/exchanges/%2f/amq.direct/publish", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq "Negative expiration not allowed"
      end
    end

    it "should require all args" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
        response = http.post("/api/exchanges/%2f/spechange/publish", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Fields .+ are required/)
      end
    end

    it "should handle string encoding" do
      with_http_server do |http, s|
        body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
        with_channel(s) do |ch|
          q = ch.queue("q2", durable: false)
          x = ch.exchange("str_enc", "topic", passive: false)
          q.bind(x.name, "*")
          response = http.post("/api/exchanges/%2f/str_enc/publish", body: body)
          response.status_code.should eq 200
          msgs = [] of AMQP::Client::DeliverMessage
          q.subscribe { |msg| msgs << msg }
          wait_for { msgs.size == 1 }
          msgs.first.not_nil!.body_io.to_s.should eq("test")
        end
      end
    end

    it "should handle base64 encoding" do
      with_http_server do |http, s|
        payload = Base64.urlsafe_encode("test")
        body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "#{payload}",
        "payload_encoding": "base64"
      })
        with_channel(s) do |ch|
          q = ch.queue("q2", durable: false)
          x = ch.exchange("str_enc", "topic", passive: false)
          q.bind(x.name, "*")
          response = http.post("/api/exchanges/%2f/str_enc/publish", body: body)
          response.status_code.should eq 200
          msgs = [] of AMQP::Client::DeliverMessage
          q.subscribe { |msg| msgs << msg }
          wait_for { msgs.size == 1 }
          msgs.first.not_nil!.body_io.to_s.should eq("test")
        end
      end
    end
  end
end
