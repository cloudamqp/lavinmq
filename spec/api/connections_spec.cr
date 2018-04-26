require "../spec_helper"

describe AvalancheMQ::ConnectionsController do
  describe "GET /api/connections" do
    it "should return connections" do
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
    it "should return connections" do
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
      end
    ensure
      h.try &.close
      s.try &.close
    end
  end
end
