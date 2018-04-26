require "../spec_helper"

describe AvalancheMQ::HTTPServer do
  FileUtils.rm_rf("/tmp/spec")

  describe "POST /api/definitions" do
    it "imports users" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.each { |u| s.users.delete(u.name) }
      spawn { h.try &.listen }
      Fiber.yield
      body = %({
        "users":[{
          "name":"sha256",
          "password_hash":"nEeL9j6VAMtdsehezoLxjI655S4vkTWs1/EJcsjVY7o",
          "hashing_algorithm":"rabbit_password_hashing_sha256","tags":""
        },
        {
          "name":"sha512",
          "password_hash":"wiwLjmFjJauaeABIerBxpPx2548gydUaqj9wpxyeio7+gmye+/KuGaLeAqrV1Tx1pk6bwYGR0gHMx+whOqxD6Q",
          "hashing_algorithm":"rabbit_password_hashing_sha512","tags":""
        },
        {
          "name":"bcrypt",
          "password_hash":"$2a$04$g5IMwYwvgDLACYdAQxCpCulKuK/Ym2I56Tz6T9Wi9DGdKQG.DE8Gi",
          "hashing_algorithm":"Bcrypt","tags":""
        },
        {
          "name":"md5",
          "password_hash":"VBxXlgu5l5QmVdFOO5YH+Q==",
          "hashing_algorithm":"rabbit_password_hashing_md5","tags":""
        }]
      })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.users.all? do |u|
        u.should be_a(AvalancheMQ::User)
        ok = u.not_nil!.password == "hej"
        "#{u.name}:#{ok}".should eq "#{u.name}:true"
      end
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports vhosts" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts.delete("def")
      body = %({ "vhosts":[{ "name":"def" }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      vhost = s.vhosts["def"]? || nil
      vhost.should be_a(AvalancheMQ::VHost)
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports queues" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_queue("q1")
      body = %({ "queues": [{ "name": "q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].queues.has_key?("q1").should be_true
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_exchange("x1")
      body = %({ "exchanges": [{ "name": "x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges.has_key?("x1").should be_true
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("x1", "direct", false, true)
      s.vhosts["/"].declare_exchange("x2", "fanout", false, true)
      s.vhosts["/"].declare_queue("q1", false, true)
      body = %({ "bindings": [
        {
          "source": "x1",
          "vhost": "/",
          "destination": "x2",
          "destination_type": "exchange",
          "routing_key": "r.k2",
          "arguments": {}
        },
        {
          "source": "x1",
          "vhost": "/",
          "destination": "q1",
          "destination_type": "queue",
          "routing_key": "rk",
          "arguments": {}
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges["x1"].matches("r.k2", nil).map(&.name).includes?("x2").should be_true
      s.vhosts["/"].exchanges["x1"].matches("rk", nil).map(&.name).includes?("q1").should be_true
      s.vhosts["/"].delete_queue("q1")
      s.vhosts["/"].delete_exchange("x1")
      s.vhosts["/"].delete_exchange("x2")
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports permissions" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.users.create("u1", "")
      body = %({ "permissions": [
        {
          "user": "u1",
          "vhost": "/",
          "configure": "c",
          "write": "w",
          "read": "r"
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.users["u1"].permissions["/"][:write].should eq(/w/)
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports policies" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({ "policies": [
        {
          "name": "p1",
          "vhost": "/",
          "apply-to": "queues",
          "priority": 1,
          "pattern": "^.*",
          "definition": {
            "x-max-length": 10
          }
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.vhosts["/"].policies.any? { |p| p.name == "p1" }.should be_true
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports parameters" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.create("guest", "guest")
      s.users.add_permission("guest", "/", /.*/, /.*/, /.*/)
      spawn { s.try &.listen(5672) }
      spawn { h.try &.listen }
      Fiber.yield
      body = %({ "parameters": [
        {
          "name": "p1",
          "component": "shovel",
          "value": {
            "src-uri": "amqp://guest:guest@localhost",
            "src-queue": "q1",
            "dest-uri": "amqp://guest:guest@localhost",
            "dest-queue": "q2"
          }
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions",
                                   headers: HTTP::Headers{"Content-Type" => "application/json"},
                                   body: body)
      response.status_code.should eq 200
      s.not_nil!.stop_shovels
      s.not_nil!.parameters.any? { |p| p.parameter_name == "p1" }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end
  end

  describe "GET /api/definitions" do
    it "exports users" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.create("guest", "guest")
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["users"].as_a.empty?.should be_false
      body["users"].all? do |u|
        (["name", "password_hash", "hashing_algorithm"] - u.as_h.keys).empty?
      end.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports vhosts" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["vhosts"].as_a.empty?.should be_false
      body["vhosts"].all? { |v| (["name"] - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports queues" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_queue("q1", false, false)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["queues"].as_a.empty?.should be_false
      body["queues"].all? do |v|
        (["name", "vhost", "auto_delete", "durable", "arguments"] - v.as_h.keys).empty?
      end.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_exchange("e1", "topic", false, false)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
      body["exchanges"].as_a.empty?.should be_false
      body["exchanges"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_exchange("x1", "direct", false, true)
      s.vhosts["/"].declare_queue("q1", false, true)
      s.vhosts["/"].bind_queue("q1", "x1", "", Hash(String, AvalancheMQ::AMQP::Field).new)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
      body["bindings"].as_a.empty?.should be_false
      body["bindings"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports permissions" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.users.create("guest", "guest")
      s.users.add_permission("guest", "/", /.*/, /.*/, /.*/)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["user", "vhost", "configure", "read", "write"]
      body["permissions"].as_a.empty?.should be_false
      body["permissions"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

     it "exports policies" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      d = JSON::Any.new({ "x-max-lenght" => 10_i64 } of String => JSON::Type)
      s.vhosts["/"].add_policy("p1", /^.*/, AvalancheMQ::Policy::Target.parse("queues"), d, -1_i8)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
      body["policies"].as_a.empty?.should be_false
      body["policies"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports parameters" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      d = JSON::Any.new({ "dummy" => 10_i64 } of String => JSON::Type)
      p = AvalancheMQ::Parameter.new("c1", "p1", d)
      s.add_parameter(p)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "component", "value"]
      body["parameters"].as_a.empty?.should be_false
      body["parameters"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end
  end

  describe "GET /api/definitions/vhost" do
    it "exports queues" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_queue("q1", false, false)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["queues"].as_a.empty?.should be_false
      body["queues"].all? do |v|
        (["name", "vhost", "auto_delete", "durable", "arguments"] - v.as_h.keys).empty?
      end.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_exchange("e1", "topic", false, false)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
      body["exchanges"].as_a.empty?.should be_false
      body["exchanges"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      s.vhosts["/"].declare_exchange("x1", "direct", false, true)
      s.vhosts["/"].declare_queue("q1", false, true)
      s.vhosts["/"].bind_queue("q1", "x1", "", Hash(String, AvalancheMQ::AMQP::Field).new)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
      body["bindings"].as_a.empty?.should be_false
      body["bindings"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end

    it "exports policies" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      d = JSON::Any.new({ "x-max-lenght" => 10_i64 } of String => JSON::Type)
      s.vhosts["/"].add_policy("p1", /^.*/, AvalancheMQ::Policy::Target.parse("queues"), d, -1_i8)
      spawn { h.try &.listen }
      Fiber.yield
      response = HTTP::Client.get("http://localhost:8080/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
      body["policies"].as_a.empty?.should be_false
      body["policies"].all? { |v| (keys - v.as_h.keys).empty? }.should be_true
    ensure
      s.try &.close
      h.try &.close
    end
  end

  describe "POST /api/definitions/vhost" do
    it "imports queues" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_queue("q1")
      body = %({ "queues": [{ "name": "q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions/%2f",
                                  headers: HTTP::Headers{"Content-Type" => "application/json"},
                                  body: body)
      response.status_code.should eq 200
      s.vhosts["/"].queues.has_key?("q1").should be_true
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports exchanges" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].delete_exchange("x1")
      body = %({ "exchanges": [{ "name": "x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
      response = HTTP::Client.post("http://localhost:8080/api/definitions/%2f",
                                  headers: HTTP::Headers{"Content-Type" => "application/json"},
                                  body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges.has_key?("x1").should be_true
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports bindings" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      s.vhosts["/"].declare_exchange("x1", "direct", false, true)
      s.vhosts["/"].declare_exchange("x2", "fanout", false, true)
      s.vhosts["/"].declare_queue("q1", false, true)
      body = %({ "bindings": [
        {
          "source": "x1",
          "vhost": "/",
          "destination": "x2",
          "destination_type": "exchange",
          "routing_key": "r.k2",
          "arguments": {}
        },
        {
          "source": "x1",
          "vhost": "/",
          "destination": "q1",
          "destination_type": "queue",
          "routing_key": "rk",
          "arguments": {}
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions/%2f",
                                  headers: HTTP::Headers{"Content-Type" => "application/json"},
                                  body: body)
      response.status_code.should eq 200
      s.vhosts["/"].exchanges["x1"].matches("r.k2", nil).map(&.name).includes?("x2").should be_true
      s.vhosts["/"].exchanges["x1"].matches("rk", nil).map(&.name).includes?("q1").should be_true
      s.vhosts["/"].delete_queue("q1")
      s.vhosts["/"].delete_exchange("x1")
      s.vhosts["/"].delete_exchange("x2")
    ensure
      h.try &.close
      s.try &.close
    end

    it "imports policies" do
      s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
      h = AvalancheMQ::HTTPServer.new(s, 8080)
      spawn { h.try &.listen }
      Fiber.yield
      body = %({ "policies": [
        {
          "name": "p1",
          "vhost": "/",
          "apply-to": "queues",
          "priority": 1,
          "pattern": "^.*",
          "definition": {
            "x-max-length": 10
          }
        }
      ]})
      response = HTTP::Client.post("http://localhost:8080/api/definitions/%2f",
                                  headers: HTTP::Headers{"Content-Type" => "application/json"},
                                  body: body)
      response.status_code.should eq 200
      s.vhosts["/"].policies.any? { |p| p.name == "p1" }.should be_true
    ensure
      h.try &.close
      s.try &.close
    end
  end
end
