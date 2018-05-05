require "../spec_helper"

describe AvalancheMQ::UsersController do
  describe "GET /api/users" do
    it "should return all users" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/users",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "password_hash", "hashing_algorithm"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/users/without-permissions" do
    it "should return users without access to any vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.create("alan", "alan")
      response = HTTP::Client.get("http://localhost:8080/api/users/without-permissions",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "POST /api/users/bulk-delete" do
    it "should delete users in bulk" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.create("alan1", "alan")
      s.users.create("alan2", "alan")
      body = %({
        "users": ["alan1", "alan2"]
      })
      response = HTTP::Client.post("http://localhost:8080/api/users/bulk-delete",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end

  describe "GET /api/users/name" do
    it "should return user" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.create("alan", "alan")
      response = HTTP::Client.get("http://localhost:8080/api/users/alan",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/users/name" do
    it "should create user with password" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.delete("alan")
      body = %({
        "password": "test"
      })
      response = HTTP::Client.put("http://localhost:8080/api/users/alan",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      u = s.users["alan"]
      ok = u.not_nil!.password == "test"
      ok.should be_true
    ensure
      h.try &.close
    end

    it "should create user with password_hash" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.delete("alan")
      body = %({
        "password_hash": "kI3GCqW5JLMJa4iX1lo7X4D6XbYqlLgxIs30+P6tENUV2POR"
      })
      response = HTTP::Client.put("http://localhost:8080/api/users/alan",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      u = s.users["alan"]
      ok = u.not_nil!.password == "test12"
      ok.should be_true
    ensure
      h.try &.close
    end

    it "should create user with empty password_hash" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.delete("alan")
      body = %({
        "password_hash": ""
      })
      response = HTTP::Client.put("http://localhost:8080/api/users/alan",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
      hrds = HTTP::Headers{"Content-Type" => "application/json",
                           "Authorization" => "Basic YWxhbjo="} # alan:
      response = HTTP::Client.get("http://localhost:8080/api/users/alan",
                                  headers: hrds)
      response.status_code.should eq 401
    ensure
      h.try &.close
    end
  end

  describe "GET /api/users/user/permissions" do
    it "should return permissions for user" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/users/guest/permissions",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "write", "read"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end
end
