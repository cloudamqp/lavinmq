require "../spec_helper"

describe AvalancheMQ::VHostsController do
  describe "GET /api/vhosts" do
    it "should return all vhosts" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "dir", "messages", "messages_unacknowledged", "messages_ready"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/vhosts/vhost" do
    it "should return vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end

    it "should return 404 if vhost does not exist" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts/404",
                                  headers: test_headers)
      response.status_code.should eq 404
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/vhosts/vhost" do
    it "should create vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.put("http://localhost:8080/api/vhosts/test",
                                  headers: test_headers)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/vhosts/vhost" do
    it "should delete vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts.create("test")
      response = HTTP::Client.delete("http://localhost:8080/api/vhosts/test",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end

  describe "GET /api/vhosts/vhost/permissions" do
    it "should return permissions for vhosts" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/vhosts/%2f/permissions",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end
end
