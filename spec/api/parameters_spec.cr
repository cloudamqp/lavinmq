require "../spec_helper"

describe AvalancheMQ::ParametersController do
  describe "GET /api/parameters" do
    it "should return all vhost scoped parameters" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/parameters",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "component", "vhost", "value"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/parameters/component" do
    it "should return all parameters for a component" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/parameters/test",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/parameters/component/vhost" do
    it "should return all parameters for a component on vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/parameters/test/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/parameters/component/vhost/name" do
    it "should return parameter" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/parameters/test/%2f/name",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/parameters/component/vhost/name" do
    it "should create parameters for a component on vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_parameter("test", "name")
      body = %({
        "value": {}
      })
      response = HTTP::Client.put("http://localhost:8080/api/parameters/test/%2f/name",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/parameters/component/vhost/name" do
    it "should delete parameter for a component on vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = HTTP::Client.delete("http://localhost:8080/api/parameters/test/%2f/name",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end

  describe "GET /api/global-parameters" do
    it "should return all global parameters" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/global-parameters",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "value"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

   describe "GET /api/global-parameters/name" do
    it "should return parameter" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = HTTP::Client.get("http://localhost:8080/api/global-parameters/name",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end

  describe "PUT /api/global-parameters/name" do
    it "should create global parameter" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.delete_parameter(nil, "name")
      body = %({
        "value": {}
      })
      response = HTTP::Client.put("http://localhost:8080/api/global-parameters/name",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/global-parameters/name" do
    it "should delete parameter" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = HTTP::Client.delete("http://localhost:8080/api/global-parameters/name",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end

  describe "GET /api/policies" do
    it "should return all policies" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      definitions = JSON::Any.new({
        "max-length" => 10_i64,
        "alternate-exchange" => "dead-letters" } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = HTTP::Client.get("http://localhost:8080/api/policies",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "vhost", "definition", "priority", "apply-to"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      h.try &.close
    end
  end

  describe "GET /api/policies/vhost" do
    it "should return policies for vhost" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      definitions = JSON::Any.new({
        "max-length" => 10_i64,
        "alternate-exchange" => "dead-letters" } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = HTTP::Client.get("http://localhost:8080/api/policies/%2f",
                                  headers: test_headers)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      h.try &.close
    end
  end

  describe "GET /api/policies/vhost/name" do
    it "should return policy" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      definitions = JSON::Any.new({
        "max-length" => 10_i64,
        "alternate-exchange" => "dead-letters" } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = HTTP::Client.get("http://localhost:8080/api/policies/%2f/test",
                                  headers: test_headers)
      response.status_code.should eq 200
    ensure
      h.try &.close
    end
  end


  describe "PUT /api/policies/vhost/name" do
    it "should create policy" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_policy("name")
      body = %({
        "apply-to": "queues",
        "priority": 4,
        "definition": { "max-length": 10 },
        "pattern": ".*"
      })
      response = HTTP::Client.put("http://localhost:8080/api/policies/%2f/name",
                                  headers: test_headers,
                                  body: body)
      response.status_code.should eq 201
    ensure
      h.try &.close
    end
  end

  describe "DELETE /api/policies/vhost/name" do
    it "should delete policy" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      definitions = JSON::Any.new({
        "max-length" => 10_i64,
        "alternate-exchange" => "dead-letters" } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = HTTP::Client.delete("http://localhost:8080/api/policies/%2f/test",
                                     headers: test_headers)
      response.status_code.should eq 204
    ensure
      h.try &.close
    end
  end
end
