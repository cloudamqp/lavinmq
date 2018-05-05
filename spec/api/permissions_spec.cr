require "../spec_helper"

describe AvalancheMQ::PermissionsController do
  describe "GET /api/permissions" do
    it "should return all permissions" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/permissions",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "read", "write"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/permissions/vhost/user" do
    it "should return all permissions for a user and vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/permissions/%2f/guest",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/permissions/vhost/user" do
    it "should create permission for a user and vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts.create("test")
      body = %({
        "configure": ".*",
        "read": ".*",
        "write": ".*"
      })
      response = HTTP::Client.put("http://localhost:8080/api/permissions/test/guest",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/permissions/vhost/user" do
    it "should delete permission for a user and vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts.create("test")
      s.users.add_permission("guest", "test", /.*/, /.*/, /.*/)
      response = HTTP::Client.delete("http://localhost:8080/api/permissions/test/guest",
                                  headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end
end
