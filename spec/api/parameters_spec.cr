require "../spec_helper"

describe AvalancheMQ::ParametersController do
  describe "GET /api/parameters" do
    it "should return all vhost scoped parameters for policymaker" do
      s, h = create_servers
      listen(h)
      s.try &.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = get("http://localhost:8080/api/parameters", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "component", "vhost", "value"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      s.try &.users.delete("arnold")
      close(h)
    end

    it "should refuse monitoring and management" do
      s, h = create_servers
      listen(h)
      s.try &.users.create("arnold", "pw", [AvalancheMQ::Tag::Management, AvalancheMQ::Tag::Monitoring])
      s.users.rm_permission("arnold", "/")
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/parameters", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.try &.users.delete("arnold")
      close(h)
    end
  end

  describe "GET /api/parameters/component" do
    it "should return all parameters for a component" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = get("http://localhost:8080/api/parameters/test")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end

  describe "GET /api/parameters/component/vhost" do
    it "should return all parameters for a component on vhost" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = get("http://localhost:8080/api/parameters/test/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end

  describe "GET /api/parameters/component/vhost/name" do
    it "should return parameter" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = get("http://localhost:8080/api/parameters/test/%2f/name")
      response.status_code.should eq 200
    ensure
      close(h)
    end
  end

  describe "PUT /api/parameters/component/vhost/name" do
    it "should create parameters for a component on vhost" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].delete_parameter("test", "name")
      body = %({
        "value": {}
      })
      response = put("http://localhost:8080/api/parameters/test/%2f/name", body: body)
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "DELETE /api/parameters/component/vhost/name" do
    it "should delete parameter for a component on vhost" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Type))
      s.vhosts["/"].add_parameter(p)
      response = delete("http://localhost:8080/api/parameters/test/%2f/name")
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "GET /api/global-parameters" do
    it "should return all global parameters" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = get("http://localhost:8080/api/global-parameters")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "value"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end
  end

  describe "GET /api/global-parameters/name" do
    it "should return parameter" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = get("http://localhost:8080/api/global-parameters/name")
      response.status_code.should eq 200
    ensure
      close(h)
    end
  end

  describe "PUT /api/global-parameters/name" do
    it "should create global parameter" do
      s, h = create_servers
      listen(h)
      s.delete_parameter(nil, "name")
      body = %({
        "value": {}
      })
      response = put("http://localhost:8080/api/global-parameters/name", body: body)
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "DELETE /api/global-parameters/name" do
    it "should delete parameter" do
      s, h = create_servers
      listen(h)
      p = AvalancheMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Type))
      s.add_parameter(p)
      response = delete("http://localhost:8080/api/global-parameters/name")
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "GET /api/policies" do
    it "should return all policies" do
      s, h = create_servers
      listen(h)
      definitions = JSON::Any.new({
        "max-length"         => 10_i64,
        "alternate-exchange" => "dead-letters",
      } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("http://localhost:8080/api/policies")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "vhost", "definition", "priority", "apply-to"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end
  end

  describe "GET /api/policies/vhost" do
    it "should return policies for vhost" do
      s, h = create_servers
      listen(h)
      definitions = JSON::Any.new({
        "max-length"         => 10_i64,
        "alternate-exchange" => "dead-letters",
      } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("http://localhost:8080/api/policies/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end

  describe "GET /api/policies/vhost/name" do
    it "should return policy" do
      s, h = create_servers
      listen(h)
      definitions = JSON::Any.new({
        "max-length"         => 10_i64,
        "alternate-exchange" => "dead-letters",
      } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = get("http://localhost:8080/api/policies/%2f/test")
      response.status_code.should eq 200
    ensure
      close(h)
    end
  end

  describe "PUT /api/policies/vhost/name" do
    it "should create policy" do
      s, h = create_servers
      listen(h)
      s.vhosts["/"].delete_policy("name")
      body = %({
        "apply-to": "queues",
        "priority": 4,
        "definition": { "max-length": 10 },
        "pattern": ".*"
      })
      response = put("http://localhost:8080/api/policies/%2f/name", body: body)
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "DELETE /api/policies/vhost/name" do
    it "should delete policy" do
      s, h = create_servers
      listen(h)
      definitions = JSON::Any.new({
        "max-length"         => 10_i64,
        "alternate-exchange" => "dead-letters",
      } of String => JSON::Type)
      s.vhosts["/"].add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
      response = delete("http://localhost:8080/api/policies/%2f/test")
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end
end
