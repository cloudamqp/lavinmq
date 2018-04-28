require "../spec_helper"
require "uri"

describe AvalancheMQ::ChannelsController do
  describe "GET /api/channels" do
    it "should return all channels" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = HTTP::Client.get("http://localhost:8080/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["vhost", "user", "number", "name", "connection_details", "state", "prefetch_count",
                "global_prefetch_count", "consumer_count", "confirm", "transactional"]
        body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    ensure
      h.try &.close
      s.try &.close
    end

    it "should return empty array if no connections" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/channels")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      h.try &.close
      s.try &.close
    end
  end

  describe "GET /api/vhosts/vhost/channels" do
    it "should return all channels for a vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    ensure
      h.try &.close
      s.try &.close
    end

    it "should return empty array if no connections" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/channels")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      h.try &.close
      s.try &.close
    end
  end

  describe "GET /api/channels/channel" do
    it "should return channel" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = HTTP::Client.get("http://localhost:8080/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = HTTP::Client.get("http://localhost:8080/api/channels/#{name}")
        response.status_code.should eq 200
      end
    ensure
      h.try &.close
      s.try &.close
    end
  end
end
