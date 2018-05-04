require "../spec_helper"

describe AvalancheMQ::ExchangesController do
  describe "GET /api/exchanges" do
    it "should return all exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["arguments", "internal", "auto_delete", "durable", "type", "vhost", "name"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost" do
    it "should return all exchanges for a vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost/name" do
    it "should return exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/amq.topic",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should return 404 if exchange does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/404",
                                  headers: test_headers)
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/exchanges/vhost/name" do
    it "should create exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic",
        "durable": false,
        "internal": false,
        "auto_delete": true,
        "arguments": {
          "alternate-exchange": "spexchange"
        }
      })
      s.vhosts["/"].delete_exchange("spechange")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should require type" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({})
      s.vhosts["/"].delete_exchange("faulty")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/faulty",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should require durable to be the same when overwriting" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic",
        "durable": true,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      s.vhosts["/"].delete_exchange("spechange")
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      body = %({
        "type": "topic",
        "durable": false,
        "arguments": {
          "alternate-exchange": "tjotjo"
        }
      })
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should not be possible to declare amq. prefixed exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic"
      })
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/amq.test",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should require config access to declare" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.create("test_perm", "pw")
      s.users.add_permission("test_perm", "/", /^$/, /^$/, /^$/)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "type": "topic"
      })
      hdrs = HTTP::Headers{"Content-Type" => "application/json",
                           "Authorization" => "Basic dGVzdF9wZXJtOnB3"}
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/test_perm",
                                  headers: hdrs,
                                  body: body)
      response.status_code.should eq 401
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/exchanges/vhost/name" do
    it "should delete exchange" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      response = HTTP::Client.delete("http://localhost:8080/api/exchanges/%2f/spechange",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end

    it "should not delete exchange if in use as source when query param if-unused is set" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "spechange", ".*")
      response = HTTP::Client.delete("http://localhost:8080/api/exchanges/%2f/spechange?if-unused=true",
                                     headers: test_headers)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should not delete exchange if in use as destination when query param if-unused is set" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      s.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
      s.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
      response = HTTP::Client.delete("http://localhost:8080/api/exchanges/%2f/spechange?if-unused=true",
                                     headers: test_headers)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/source" do
    it "should list bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "spechange", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/spechange/bindings/source",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.size.should eq 1
    ensure
      h.try &.close
    end
  end

  describe "GET /api/exchanges/vhost/name/bindings/destination" do
    it "should list bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      s.vhosts["/"].declare_exchange("spechange2", "topic", false, false)
      s.vhosts["/"].bind_exchange("spechange", "spechange2", ".*")
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/spechange/bindings/destination",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.size.should eq 1
    ensure
      h.try &.close
    end
  end

  describe "POST /api/exchanges/vhost/name/publish" do
    it "should publish" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("spechange", "topic", false, false)
      s.vhosts["/"].declare_queue("q1", false, false)
      s.vhosts["/"].bind_queue("q1", "spechange", "*")
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
      response = HTTP::Client.post("http://localhost:8080/api/exchanges/%2f/spechange/publish",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["routed"].as_bool.should be_true
      s.vhosts["/"].queues["q1"].message_count.should eq 1
    ensure
      s.try &.vhosts["/"].delete_queue("q1")
      s.try &.vhosts["/"].delete_exchange("spechange")
      h.try &.close
    end

    it "should require all args" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({})
      response = HTTP::Client.post("http://localhost:8080/api/exchanges/%2f/spechange/publish",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 400
    ensure
      h.try &.close
    end

    it "should handle string encoding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      spawn { s.try &.listen(5672) }
      Fiber.yield
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "test",
        "payload_encoding": "string"
      })
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q2", durable: false)
        x = ch.exchange("str_enc", "topic", passive: false)
        q.bind(x, "*")
        response = HTTP::Client.post("http://localhost:8080/api/exchanges/%2f/str_enc/publish",
                                  headers: test_headers,
                                  body: body)
        response.status_code.should eq 200
        msgs = [] of AMQP::Message
        q.subscribe { |msg| msgs << msg }
        wait_for { msgs.size == 1 }
        msgs[0].to_s.should eq("test")
      end
    ensure
      s.try &.vhosts["/"].delete_queue("q2")
      s.try &.vhosts["/"].delete_exchange("str_enc")
      h.try &.close
      s.try &.close
    end

    it "should handle base64 encoding" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      spawn { s.try &.listen(5672) }
      Fiber.yield
      payload = Base64.urlsafe_encode("test")
      body = %({
        "properties": {},
        "routing_key": "rk",
        "payload": "#{payload}",
        "payload_encoding": "base64"
      })
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("q2", durable: false)
        x = ch.exchange("str_enc", "topic", passive: false)
        q.bind(x, "*")
        response = HTTP::Client.post("http://localhost:8080/api/exchanges/%2f/str_enc/publish",
                                  headers: test_headers,
                                  body: body)
        response.status_code.should eq 200
        msgs = [] of AMQP::Message
        q.subscribe { |msg| msgs << msg }
        wait_for { msgs.size == 1 }
        msgs[0].to_s.should eq("test")
      end
    ensure
      s.try &.vhosts["/"].delete_queue("q2")
      s.try &.vhosts["/"].delete_exchange("str_enc")
      h.try &.close
      s.try &.close
    end
  end
end
