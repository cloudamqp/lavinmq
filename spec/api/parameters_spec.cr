require "../spec_helper"

describe LavinMQ::HTTP::ParametersController do
  describe "GET /api/parameters" do
    it "should return all vhost scoped parameters for policymaker" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
        s.vhosts["/"].add_parameter(p)
        response = http.get("/api/parameters", headers: hdrs)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "component", "vhost", "value"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "should refuse monitoring and management" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management, LavinMQ::Tag::Monitoring])
        s.users.rm_permission("arnold", "/")
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/parameters", headers: hdrs)
        response.status_code.should eq 403
      end
    end
  end

  describe "GET /api/parameters/component" do
    it "should return all parameters for a component" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
        s.vhosts["/"].add_parameter(p)
        response = http.get("/api/parameters/test")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
  describe "GET /api/parameters/component/vhost" do
    it "should return all parameters for a component on vhost" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
        s.vhosts["/"].add_parameter(p)
        response = http.get("/api/parameters/test/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
  describe "GET /api/parameters/component/vhost/name" do
    it "should return parameter" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
        s.vhosts["/"].add_parameter(p)
        response = http.get("/api/parameters/test/%2f/name")
        response.status_code.should eq 200
      end
    end
  end
  describe "PUT /api/parameters/component/vhost/name" do
    it "should create parameters for a component on vhost" do
      with_http_server do |http, s|
        body = %({
        "value": { "key": "value" }
      })
        response = http.put("/api/parameters/test/%2f/name", body: body)
        response.status_code.should eq 201
        s.vhosts["/"].parameters[{"test", "name"}].value.should eq({"key" => "value"})
      end
    end

    it "should update parameters for a component on vhost" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new(%({ "key": "old value" })))
        s.vhosts["/"].add_parameter(p)
        body = %({
        "value": { "key": "new value" }
      })
        response = http.put("/api/parameters/test/%2f/name", body: body)
        response.status_code.should eq 204
        s.vhosts["/"].parameters[{"test", "name"}].value.should eq({"key" => "new value"})
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.put("/api/parameters/test/%2f/name", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field .+ is required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/parameters/test/%2f/name", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/parameters/test/%2f/name", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
  end
  describe "DELETE /api/parameters/component/vhost/name" do
    it "should delete parameter for a component on vhost" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new("test", "name", JSON::Any.new({} of String => JSON::Any))
        s.vhosts["/"].add_parameter(p)
        response = http.delete("/api/parameters/test/%2f/name")
        response.status_code.should eq 204
      end
    end
  end

  describe "PUT /api/parameters/shovel/vhost/name" do
    it "should validate shovel config with valid config" do
      with_http_server do |http, s|
        s.users.add_permission("guest", "/", /.*/, /.*/, /.*/)
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body)
        response.status_code.should eq 201
      end
    end

    it "should reject shovel without source queue or exchange" do
      with_http_server do |http, s|
        s.users.add_permission("guest", "/", /.*/, /.*/, /.*/)
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("source requires a queue or an exchange")
      end
    end

    it "should reject shovel without destination" do
      with_http_server do |http, s|
        s.users.add_permission("guest", "/", /.*/, /.*/, /.*/)
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("destination requires queue and/or exchange")
      end
    end

    it "should reject shovel when user lacks write permission on destination exchange" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /.*/, /.*/, /^$/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-exchange": "dest-exchange"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access exchange")
      end
    end

    it "should reject shovel when user lacks config permission on destination exchange" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^$/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-exchange": "dest-exchange"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access exchange")
      end
    end

    it "should reject shovel when user lacks config permission on destination queue" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^(?!dest-queue$).*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-exchange": "dest-exchange",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access queue")
      end
    end

    it "should reject shovel when user lacks read permission on source queue" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /.*/, /^$/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access queue")
      end
    end

    it "should reject shovel when user lacks config permission on source queue" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^(?!source-queue$).*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "dest-exchange": "dest-exchange"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access queue")
      end
    end

    it "should reject shovel when user lacks read permission on source exchange" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /.*/, /^(?!source-exchange$).*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-exchange": "source-exchange",
            "dest-exchange": "dest-exchange"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access exchange")
      end
    end

    it "should reject shovel when user lacks config permission on source exchange" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^$/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "src-exchange": "source-exchange",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should contain("can't access exchange")
      end
    end

    it "should allow shovel with user having sufficient permissions" do
      with_http_server do |http, s|
        s.users.create("sufficient", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("sufficient", "/", /.*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic c3VmZmljaWVudDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://",
            "src-queue": "source-queue",
            "src-exchange": "source-exchange",
            "dest-exchange": "dest-exchange",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 201
      end
    end

    it "should skip validation for external URIs" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^$/, /^$/, /^$/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://external-host",
            "dest-uri": "amqp://external-host",
            "src-queue": "source-queue",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 201
      end
    end

    it "should skip validation for URIs with explicit user" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^$/, /^$/, /^$/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://someuser:pass@localhost",
            "dest-uri": "amqp://someuser:pass@localhost",
            "src-queue": "source-queue",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 201
      end
    end

    it "should allow shovel with source exchange and routing key without source queue" do
      with_http_server do |http, s|
        s.users.create("limited", "pw", [LavinMQ::Tag::PolicyMaker])
        s.users.add_permission("limited", "/", /^amq\.topic$/, /^amq\.topic$/, /^$/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic bGltaXRlZDpwdw=="}
        body = %({
          "value": {
            "src-uri": "amqp://",
            "dest-uri": "amqp://external-host",
            "src-exchange": "amq.topic",
            "src-exchange-key": "#",
            "dest-queue": "dest-queue"
          }
        })
        response = http.put("/api/parameters/shovel/%2f/test-shovel", body: body, headers: hdrs)
        response.status_code.should eq 201
      end
    end
  end

  describe "GET /api/global-parameters" do
    it "should return all global parameters" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
        s.add_parameter(p)
        response = http.get("/api/global-parameters")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "value"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end
  describe "GET /api/global-parameters/name" do
    it "should return parameter" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
        s.add_parameter(p)
        response = http.get("/api/global-parameters/name")
        response.status_code.should eq 200
      end
    end
  end
  describe "PUT /api/global-parameters/name" do
    it "should create global parameter" do
      with_http_server do |http, s|
        s.delete_parameter(nil, "name")
        body = %({
        "value": {}
      })
        response = http.put("/api/global-parameters/name", body: body)
        response.status_code.should eq 201
      end
    end

    it "should update global parameter" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new(nil, "name", JSON::Any.new(%({ "key": "old value" })))
        s.add_parameter(p)
        body = %({
        "value": { "key": "new value" }
      })
        response = http.put("/api/global-parameters/name", body: body)
        response.status_code.should eq 204
        s.parameters[{nil, "name"}].value.should eq({"key" => "new value"})
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.put("/api/global-parameters/name", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field .+ is required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/global-parameters/name", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/global-parameters/name", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
  end
  describe "DELETE /api/global-parameters/name" do
    it "should delete parameter" do
      with_http_server do |http, s|
        p = LavinMQ::Parameter.new(nil, "name", JSON::Any.new({} of String => JSON::Any))
        s.add_parameter(p)
        response = http.delete("/api/global-parameters/name")
        response.status_code.should eq 204
      end
    end
  end
  describe "GET /api/policies" do
    it "should return all policies" do
      with_http_server do |http, s|
        definitions = {
          "max-length"         => JSON::Any.new(10_i64),
          "alternate-exchange" => JSON::Any.new("dead-letters"),
        }
        s.vhosts["/"].add_policy("test", "^.*$", "all", definitions, -10_i8)
        response = http.get("/api/policies")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "vhost", "definition", "priority", "apply-to"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end
  describe "GET /api/policies/vhost" do
    it "should return policies for vhost" do
      with_http_server do |http, s|
        definitions = {
          "max-length"         => JSON::Any.new(10_i64),
          "alternate-exchange" => JSON::Any.new("dead-letters"),
        }
        s.vhosts["/"].add_policy("test", "^.*$", "all", definitions, -10_i8)
        response = http.get("/api/policies/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
  describe "GET /api/policies/vhost/name" do
    it "should return policy" do
      with_http_server do |http, s|
        definitions = {
          "max-length"         => JSON::Any.new(10_i64),
          "alternate-exchange" => JSON::Any.new("dead-letters"),
        }
        s.vhosts["/"].add_policy("test", "^.*$", "all", definitions, -10_i8)
        response = http.get("/api/policies/%2f/test")
        response.status_code.should eq 200
      end
    end
  end
  describe "PUT /api/policies/vhost/name" do
    it "should create policy" do
      with_http_server do |http, s|
        body = %({
        "apply-to": "queues",
        "priority": 4,
        "definition": { "max-length": 10 },
        "pattern": ".*"
      })
        response = http.put("/api/policies/%2f/name", body: body)
        response.status_code.should eq 201
        s.vhosts["/"].policies["name"].definition["max-length"].as_i.should eq 10
      end
    end

    it "should update policy" do
      with_http_server do |http, s|
        policy_name = "test"
        definitions = {"max-length" => JSON::Any.new(10_i64)}
        s.vhosts["/"].add_policy(policy_name, "^.*$", "all", definitions, -10_i8)

        body = %({
        "pattern": ".*",
        "definition": { "max-length": 20 }
      })
        response = http.put("/api/policies/%2f/#{policy_name}", body: body)
        response.status_code.should eq 204
        s.vhosts["/"].policies[policy_name].definition["max-length"].as_i.should eq 20
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.put("/api/policies/%2f/name", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Fields .+ are required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/policies/%2f/name", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/policies/%2f/name", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end

    it "should handle invalid definition types" do
      with_http_server do |http, _|
        body = %({
        "apply-to": "queues",
        "priority": 0,
        "definition": { "max-length": "String" },
        "pattern": ".*"
      })
        response = http.put("/api/policies/%2f/name", body: body)
        response.status_code.should eq 400
      end
    end
  end

  describe "PUT /api/operator-policies/vhost/name" do
    it "should handle invalid definition types" do
      with_http_server do |http, _|
        body = %({
        "apply-to": "queues",
        "priority": 0,
        "definition": { "max-length": "String" },
        "pattern": ".*"
      })
        response = http.put("/api/operator-policies/%2f/name", body: body)
        response.status_code.should eq 400
      end
    end
  end
  describe "DELETE /api/policies/vhost/name" do
    it "should delete policy" do
      with_http_server do |http, s|
        definitions = {
          "max-length"         => JSON::Any.new(10_i64),
          "alternate-exchange" => JSON::Any.new("dead-letters"),
        }
        s.vhosts["/"].add_policy("test", "^.*$", "all", definitions, -10_i8)
        response = http.delete("/api/policies/%2f/test")
        response.status_code.should eq 204
      end
    end
  end
  describe "PUT /api/operator-policies/vhost/name" do
    it "should handle invalid definition types" do
      with_http_server do |http, _|
        body = %({
        "apply-to": "queues",
        "priority": 0,
        "definition": { "max-length": "String" },
        "pattern": ".*"
      })
        response = http.put("/api/operator-policies/%2f/name", body: body)
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/should be of type Int/)
      end
    end
  end
end
