require "../spec_helper"
require "uri"

describe AvalancheMQ::ConnectionsController do
  describe "GET /api/connections" do
    it "should return network connections" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        response = HTTP::Client.get("http://localhost:8080/api/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    ensure
      h.try &.close
      s.try &.close
    end
  end

  describe "GET /api/vhosts/vhost/connections" do
    it "should return network connections" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["channels", "connected_at", "type", "channel_max", "timeout", "client_properties",
                "vhost", "user", "protocol", "auth_mechanism", "host", "port", "name", "ssl",
                "state"]
        body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    ensure
      h.try &.close
      s.try &.close
    end

    it "should return 404 if vhosts does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts/vhost/connections")
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "GET /api/connections/name" do
    it "should return connection" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = HTTP::Client.get("http://localhost:8080/api/connections/#{name}")
        response.status_code.should eq 200
      end
    ensure
      h.try &.close
      s.try &.close
    end

    it "should return 404 if connection does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/connections/name")
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/connections/name" do
    it "should close connection" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      AMQP::Connection.start do |conn|
        response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = HTTP::Client.delete("http://localhost:8080/api/connections/#{name}")
      ensure
        response.try &.status_code.should eq 200
      end
    ensure
      h.try &.close
      s.try &.close
    end
  end
end
