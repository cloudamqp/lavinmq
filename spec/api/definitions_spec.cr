require "../spec_helper"

describe LavinMQ::HTTP::Server do
  describe "POST /api/definitions" do
    it "imports users" do
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
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.users.select("sha256", "sha512", "bcrypt", "md5").each do |_, u|
        u.should be_a(LavinMQ::User)
        ok = u.not_nil!.password.not_nil!.verify "hej"
        {u.name, ok}.should(eq({u.name, true}))
      end
    end

    it "imports vhosts" do
      Server.vhosts.delete("def")
      body = %({ "vhosts":[{ "name":"def" }] })
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      vhost = Server.vhosts["def"]?
      vhost.should be_a(LavinMQ::VHost)
    end

    # https://github.com/cloudamqp/lavinmq/issues/276
    context "if default user has been replaced" do
      before_each do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::Administrator], save: false) # Will be the new default_user
      end

      after_each do
        Server.users.delete("other_name", save: false)
        Server.vhosts.delete("new")
        Server.users.create("guest", "guest", [LavinMQ::Tag::Administrator])
        Server.vhosts.each_key { |name| Server.users.add_permission("guest", name, /.*/, /.*/, /.*/) }
      end

      it "imports with new default user" do
        headers = HTTP::Headers{"Content-Type"  => "application/json",
                                "Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="} # other_name:guest
        body = %({ "vhosts":[{ "name":"new" }] })
        response = post("/api/definitions", body: body, headers: headers)
        response.status_code.should eq 200
        Server.vhosts["new"]?.should be_a(LavinMQ::VHost)
      end
    end

    it "imports queues" do
      body = %({ "queues": [{ "name": "import_q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].queues.has_key?("import_q1").should be_true
    end

    it "imports exchanges" do
      body = %({ "exchanges": [{ "name": "import_x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].exchanges.has_key?("import_x1").should be_true
    end

    it "imports bindings" do
      Server.vhosts["/"].declare_exchange("import_x1", "topic", false, true)
      Server.vhosts["/"].declare_exchange("import_x2", "fanout", false, true)
      Server.vhosts["/"].declare_queue("import_q1", false, true)
      body = %({ "bindings": [
        {
          "source": "import_x1",
          "vhost": "/",
          "destination": "import_x2",
          "destination_type": "exchange",
          "routing_key": "r.k2",
          "arguments": {}
        },
        {
          "source": "import_x1",
          "vhost": "/",
          "destination": "import_q1",
          "destination_type": "queue",
          "routing_key": "rk",
          "arguments": {}
        }
      ]})
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      matches = [] of String
      ex = Server.vhosts["/"].exchanges["import_x1"]
      ex.do_exchange_matches("r.k2", nil) { |e| matches << e.name }
      matches.includes?("import_x2").should be_true
      matches.clear
      ex.do_queue_matches("rk", nil) { |e| matches << e.name }
      matches.includes?("import_q1").should be_true
    end

    it "imports permissions" do
      Server.users.create("u1", "")
      body = %({ "permissions": [
        {
          "user": "u1",
          "vhost": "/",
          "configure": "c",
          "write": "w",
          "read": "r"
        }
      ]})
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.users["u1"].permissions["/"][:write].should eq(/w/)
    end

    it "imports policies" do
      body = %({ "policies": [
        {
          "name": "import_p1",
          "vhost": "/",
          "apply-to": "queues",
          "priority": 1,
          "pattern": "^.*",
          "definition": {
            "x-max-length": 10
          }
        }
      ]})
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].policies.has_key?("import_p1").should be_true
    end

    it "imports parameters" do
      body = %({ "parameters": [
        {
          "name": "import_shovel_param",
          "component": "shovel",
          "vhost": "/",
          "value": {
            "src-uri": "#{AMQP_BASE_URL}",
            "src-queue": "shovel_will_declare_q1",
            "dest-uri": "#{AMQP_BASE_URL}",
            "dest-queue": "shovel_will_declare_q1"
          }
        }
      ]})
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      # Because we run shovels in a new Fiber we have to make sure the shovel is not started
      # after this spec has finished
      sleep 0.1 # Start the shovel
      wait_for do
        shovels = Server.vhosts["/"].shovels.not_nil!
        shovels.each_value.all? &.running?
      end
      Server.vhosts["/"].parameters.any? { |_, p| p.parameter_name == "import_shovel_param" }
        .should be_true
    end

    it "imports global parameters" do
      body = %({ "global_parameters": [
        {
          "name": "global_p1",
          "value": {}
        }
      ]})
      response = post("/api/definitions", body: body)
      response.status_code.should eq 200
      Server.parameters.any? { |_, p| p.parameter_name == "global_p1" }.should be_true
    end

    it "should handle request with empty body" do
      response = post("/api/definitions", body: "")
      response.status_code.should eq 200
    end

    it "should handle unexpected input" do
      response = post("/api/definitions", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Input needs to be a JSON object.")
    end

    it "should handle invalid JSON" do
      response = post("/api/definitions", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end
  end

  describe "GET /api/definitions" do
    it "exports users" do
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["users"].as_a.empty?.should be_false
      keys = ["name", "password_hash", "hashing_algorithm"]
      body["users"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports vhosts" do
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["vhosts"].as_a.empty?.should be_false
      keys = ["name"]
      body["vhosts"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports queues" do
      Server.vhosts["/"].declare_queue("export_q1", false, false)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["queues"].as_a.empty?.should be_false
      keys = ["name", "vhost", "auto_delete", "durable", "arguments"]
      body["queues"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports exchanges" do
      Server.vhosts["/"].declare_exchange("export_e1", "topic", false, false)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
      body["exchanges"].as_a.empty?.should be_false
      body["exchanges"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports bindings" do
      Server.vhosts["/"].declare_exchange("export_x1", "direct", false, true)
      Server.vhosts["/"].declare_queue("export_q1", false, true)
      Server.vhosts["/"].bind_queue("export_q1", "export_x1", "", AMQ::Protocol::Table.new)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
      body["bindings"].as_a.empty?.should be_false
      body["bindings"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports permissions" do
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["user", "vhost", "configure", "read", "write"]
      body["permissions"].as_a.empty?.should be_false
      body["permissions"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports policies" do
      d = {"x-max-lenght" => JSON::Any.new(10_i64)}
      Server.vhosts["/"].add_policy("export_p1", "^.*", "queues", d, -1_i8)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
      body["policies"].as_a.empty?.should be_false
      body["policies"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports global parameters" do
      d = JSON::Any.new({"dummy" => JSON::Any.new(10_i64)})
      p = LavinMQ::Parameter.new("c1", "p11", d)
      Server.add_parameter(p)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "component", "value"]
      body["global_parameters"].as_a.empty?.should be_false
      body["global_parameters"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports vhost parameters" do
      d = JSON::Any.new({"dummy" => JSON::Any.new(10_i64)})
      p = LavinMQ::Parameter.new("c1", "p11", d)
      Server.vhosts["/"].add_parameter(p)
      response = get("/api/definitions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "component", "value"]
      body["parameters"].as_a.empty?.should be_false
      body["parameters"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end
  end

  describe "GET /api/definitions/vhost" do
    it "exports queues" do
      Server.vhosts["/"].declare_queue("export_q2", false, false)
      response = get("/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body["queues"].as_a.empty?.should be_false
      keys = ["name", "vhost", "auto_delete", "durable", "arguments"]
      body["queues"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports exchanges" do
      Server.vhosts["/"].declare_exchange("export_e2", "topic", false, false)
      response = get("/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
      body["exchanges"].as_a.empty?.should be_false
      body["exchanges"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports bindings" do
      Server.vhosts["/"].declare_exchange("export_x1", "direct", false, true)
      Server.vhosts["/"].declare_queue("export_q1", false, true)
      Server.vhosts["/"].bind_queue("export_q1", "export_x1", "", AMQ::Protocol::Table.new)
      response = get("/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
      body["bindings"].as_a.empty?.should be_false
      body["bindings"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "exports policies" do
      d = {"x-max-lenght" => JSON::Any.new(10_i64)}
      Server.vhosts["/"].add_policy("export_p2", "^.*", "queues", d, -1_i8)
      response = get("/api/definitions/%2f")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
      body["policies"].as_a.empty?.should be_false
      body["policies"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    describe "user tags and vhost access" do
      it "export vhost definitions as management user" do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
        Server.vhosts.create("new")
        Server.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
        headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
        response = get("/api/definitions/new", headers: headers)
        response.status_code.should eq 200
      end

      it "should refuse vhost access" do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
        Server.vhosts.create("new")
        headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
        response = get("/api/definitions/new", headers: headers)
        response.status_code.should eq 401
        body = JSON.parse(response.body)
        body["reason"].should eq "Access refused"
      end
    end
  end

  describe "POST /api/definitions/vhost" do
    it "imports queues" do
      body = %({ "queues": [{ "name": "import_q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
      response = post("/api/definitions/%2f", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].queues.has_key?("import_q1").should be_true
    end

    it "imports exchanges" do
      body = %({ "exchanges": [{ "name": "import_x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
      response = post("/api/definitions/%2f", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].exchanges.has_key?("import_x1").should be_true
    end

    it "imports bindings" do
      Server.vhosts["/"].declare_exchange("import_x1", "direct", false, true)
      Server.vhosts["/"].declare_exchange("import_x2", "fanout", false, true)
      Server.vhosts["/"].declare_queue("import_q1", false, true)
      body = %({ "bindings": [
        {
          "source": "import_x1",
          "vhost": "/",
          "destination": "import_x2",
          "destination_type": "exchange",
          "routing_key": "r.k2",
          "arguments": {}
        },
        {
          "source": "import_x1",
          "vhost": "/",
          "destination": "import_q1",
          "destination_type": "queue",
          "routing_key": "rk",
          "arguments": {}
        }
      ]})
      response = post("/api/definitions/%2f", body: body)
      response.status_code.should eq 200
      matches = [] of String
      ex = Server.vhosts["/"].exchanges["import_x1"]
      ex.do_exchange_matches("r.k2", nil) { |e| matches << e.name }
      matches.includes?("import_x2").should be_true
      matches.clear
      ex.do_queue_matches("rk", nil) { |e| matches << e.name }
      matches.includes?("import_q1").should be_true
    end

    it "imports policies" do
      body = %({ "policies": [
        {
          "name": "import_p1",
          "vhost": "/",
          "apply-to": "queues",
          "priority": 1,
          "pattern": "^.*",
          "definition": {
            "x-max-length": 10
          }
        }
      ]})
      response = post("/api/definitions/%2f", body: body)
      response.status_code.should eq 200
      Server.vhosts["/"].policies.has_key?("import_p1").should be_true
    end

    it "should handle request with empty body" do
      response = post("/api/definitions/%2f", body: "")
      response.status_code.should eq 200
    end

    it "should handle unexpected input" do
      response = post("/api/definitions/%2f", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Input needs to be a JSON object.")
    end

    it "should handle invalid JSON" do
      response = post("/api/definitions/%2f", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end

    describe "user tags and vhost access" do
      it "import vhost definitions as policymaker user" do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::PolicyMaker], save: false) # Will be the new default_user
        Server.vhosts.create("new")
        Server.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
        headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
        body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
        response = post("/api/definitions/new", headers: headers, body: body)
        response.status_code.should eq 200
        Server.vhosts["new"].queues.has_key?("import_q1").should be_true
      end

      it "should refuse vhost access" do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
        Server.vhosts.create("new")
        headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
        body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
        response = post("/api/definitions/new", headers: headers, body: body)
        response.status_code.should eq 401
        body = JSON.parse(response.body)
        body["reason"].should eq "Access refused"
      end

      it "should refuse user tag access" do
        Server.users.delete("guest")
        Server.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
        Server.vhosts.create("new")
        Server.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
        headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
        body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
        response = post("/api/definitions/new", headers: headers, body: body)
        response.status_code.should eq 401
        body = JSON.parse(response.body)
        body["reason"].should eq "Access refused"
      end
    end
  end

  describe "POST /api/definitions/upload" do
    it "imports definitions from uploaded file (no Referer)" do
      file_content = %({ "vhosts":[{ "name":"uploaded_vhost" }] }) # sanity check
      io = IO::Memory.new
      builder = HTTP::FormData::Builder.new(io)
      builder.file("file", IO::Memory.new(file_content))
      builder.finish

      headers = {"Content-Type" => builder.content_type}
      body = io.to_s

      response = post("/api/definitions/upload", headers: headers, body: body)
      response.status_code.should eq 200
      Server.vhosts["uploaded_vhost"]?.should_not be_nil
      Server.vhosts["uploaded_vhost"].should be_a(LavinMQ::VHost)
    end

    it "imports definitions from uploaded file" do
      file_content = %({ "vhosts":[{ "name":"uploaded_vhost" }] }) # sanity check
      io = IO::Memory.new
      builder = HTTP::FormData::Builder.new(io)
      builder.file("file", IO::Memory.new(file_content))
      builder.finish

      headers = {"Content-Type" => builder.content_type, "Referer" => "/foo"}
      body = io.to_s

      response = post("/api/definitions/upload", headers: headers, body: body)
      response.status_code.should eq 302
      response.headers["Location"].should eq "/foo"
      Server.vhosts["uploaded_vhost"]?.should_not be_nil
      Server.vhosts["uploaded_vhost"].should be_a(LavinMQ::VHost)
    end
  end

  it "should update existing user on import" do
    name = "bcryptuser"
    body = %({
      "users":[{
        "name":"#{name}",
        "password_hash":"$2a$04$g5IMwYwvgDLACYdAQxCpCulKuK/Ym2I56Tz6T9Wi9DGdKQG.DE8Gi",
        "hashing_algorithm":"Bcrypt","tags":""
      }]
    })

    response = post("/api/definitions", body: body)
    response.status_code.should eq 200

    u = Server.users[name]
    u.should be_a(LavinMQ::User)
    ok = u.not_nil!.password.not_nil!.verify "hej"
    {u.name, ok}.should eq({name, true})

    update_body = %({
      "users":[{
        "name":"#{name}",
        "password_hash":"$2a$04$PuoK2zgHy/NHRU3CRUCidOKaSTwFkv97Sm.zTspKZRWJkn6l37YOe",
        "hashing_algorithm":"Bcrypt","tags":""
      }]
    })
    response = post("/api/definitions", body: update_body)
    response.status_code.should eq 200

    u = Server.users[name]
    u.should be_a(LavinMQ::User)
    ok = u.not_nil!.password.not_nil!.verify "test"
    {u.name, ok}.should eq({name, true})
  end
end
