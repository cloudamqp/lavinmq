require "../spec_helper"

describe AvalancheMQ::ExchangesController do
  describe "GET /api/exchanges" do
    it "should return all exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges")
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
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f")
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
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/amq.topic")
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should return 404 if exchange does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/404")
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
      response = HTTP::Client.put("http://localhost:8080/api/exchanges/%2f/spechange",
                                  headers: HTTP::Headers{"Content-Type" => "application/json"},
                                  body: body)
      response.status_code.should eq 204
      response = HTTP::Client.get("http://localhost:8080/api/exchanges/%2f/spechange")
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end
end
