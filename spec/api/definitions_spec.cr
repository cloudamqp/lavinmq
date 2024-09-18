require "../spec_helper"

describe LavinMQ::HTTP::Server do
  describe "POST /api/definitions" do
    it "imports users" do
      with_http_server do |http, s|
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
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.users.select("sha256", "sha512", "bcrypt", "md5").each do |_, u|
          u.should be_a(LavinMQ::User)
          ok = u.not_nil!.password.not_nil!.verify "hej"
          {u.name, ok}.should(eq({u.name, true}))
        end
      end
    end

    it "imports vhosts" do
      with_http_server do |http, s|
        s.vhosts.delete("def")
        body = %({ "vhosts":[{ "name":"def" }] })
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        vhost = s.vhosts["def"]?
        vhost.should be_a(LavinMQ::VHost)
      end
    end

    # https://github.com/cloudamqp/lavinmq/issues/276
    context "if default user has been replaced" do
      it "imports with new default user" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::Administrator], save: false) # Will be the new default_user
          headers = HTTP::Headers{"Content-Type"  => "application/json",
                                  "Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="} # other_name:guest
          body = %({ "vhosts":[{ "name":"new" }] })
          response = http.post("/api/definitions", body: body, headers: headers)
          response.status_code.should eq 200
          s.vhosts["new"]?.should be_a(LavinMQ::VHost)
        end
      end
    end

    it "imports queues" do
      with_http_server do |http, s|
        body = %({ "queues": [{ "name": "import_q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].queues.has_key?("import_q1").should be_true
      end
    end

    it "imports exchanges" do
      with_http_server do |http, s|
        body = %({ "exchanges": [{ "name": "import_x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].exchanges.has_key?("import_x1").should be_true
      end
    end

    it "imports bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("import_x1", "topic", false, true)
        s.vhosts["/"].declare_exchange("import_x2", "fanout", false, true)
        s.vhosts["/"].declare_queue("import_q1", false, true)
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
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        ex = s.vhosts["/"].exchanges["import_x1"]
        qs = Set(LavinMQ::Queue).new
        es = Set(LavinMQ::Exchange).new
        ex.find_queues("r.k2", nil, qs, es)
        res = Set(LavinMQ::Exchange).new
        res << s.vhosts["/"].exchanges["import_x1"]
        res << s.vhosts["/"].exchanges["import_x2"]
        es.should eq res
        qs = Set(LavinMQ::Queue).new
        es = Set(LavinMQ::Exchange).new
        ex.find_queues("rk", nil, qs, es)
        res = Set(LavinMQ::Queue).new
        res << s.vhosts["/"].queues["import_q1"]
        qs.should eq res
      end
    end

    it "imports permissions" do
      with_http_server do |http, s|
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
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.users["u1"].permissions["/"][:write].should eq(/w/)
      end
    end

    it "imports policies" do
      with_http_server do |http, s|
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
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].policies.has_key?("import_p1").should be_true
      end
    end

    it "imports parameters" do
      with_http_server do |http, s|
        body = %({ "parameters": [
          {
            "name": "import_shovel_param",
            "component": "shovel",
            "vhost": "/",
            "value": {
              "src-uri": "#{s.amqp_url}",
              "src-queue": "shovel_will_declare_q1",
              "dest-uri": "#{s.amqp_url}",
              "dest-queue": "shovel_will_declare_q1"
            }
          }
        ]})
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        # Because we run shovels in a new Fiber we have to make sure the shovel is not started
        # after this spec has finished
        sleep 0.1.seconds # Start the shovel
        wait_for do
          shovels = s.vhosts["/"].shovels.not_nil!
          shovels.each_value.all? &.running?
        end
        s.vhosts["/"].parameters.any? { |_, p| p.parameter_name == "import_shovel_param" }
          .should be_true
      end
    end

    it "imports global parameters" do
      with_http_server do |http, s|
        body = %({ "global_parameters": [
        {
          "name": "global_p1",
          "value": {}
        }
      ]})
        response = http.post("/api/definitions", body: body)
        response.status_code.should eq 200
        s.parameters.any? { |_, p| p.parameter_name == "global_p1" }.should be_true
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.post("/api/definitions", body: "")
        response.status_code.should eq 200
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.post("/api/definitions", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.post("/api/definitions", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
  end
  describe "GET /api/definitions" do
    it "exports users" do
      with_http_server do |http, _|
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["users"].as_a.empty?.should be_false
        keys = ["name", "password_hash", "hashing_algorithm"]
        bad_keys = ["permissions"]
        body["users"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        body["users"].as_a.each { |v| bad_keys.each { |k| v.as_h.keys.should_not contain(k) } }
      end
    end

    it "exports vhosts" do
      with_http_server do |http, _|
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["vhosts"].as_a.empty?.should be_false
        keys = ["name"]
        body["vhosts"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports queues" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("export_q1", false, false)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["queues"].as_a.empty?.should be_false
        keys = ["name", "vhost", "auto_delete", "durable", "arguments"]
        body["queues"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports exchanges" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("export_e1", "topic", false, false)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
        body["exchanges"].as_a.empty?.should be_false
        body["exchanges"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("export_x1", "direct", false, true)
        s.vhosts["/"].declare_queue("export_q1", false, true)
        s.vhosts["/"].bind_queue("export_q1", "export_x1", "", AMQ::Protocol::Table.new)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
        body["bindings"].as_a.empty?.should be_false
        body["bindings"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports permissions" do
      with_http_server do |http, _|
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["user", "vhost", "configure", "read", "write"]
        body["permissions"].as_a.empty?.should be_false
        body["permissions"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports policies" do
      with_http_server do |http, s|
        d = {"x-max-lenght" => JSON::Any.new(10_i64)}
        s.vhosts["/"].add_policy("export_p1", "^.*", "queues", d, -1_i8)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
        body["policies"].as_a.empty?.should be_false
        body["policies"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports global parameters" do
      with_http_server do |http, s|
        d = JSON::Any.new({"dummy" => JSON::Any.new(10_i64)})
        p = LavinMQ::Parameter.new("c1", "p11", d)
        s.add_parameter(p)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "component", "value"]
        body["global_parameters"].as_a.empty?.should be_false
        body["global_parameters"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports vhost parameters" do
      with_http_server do |http, s|
        d = JSON::Any.new({"dummy" => JSON::Any.new(10_i64)})
        p = LavinMQ::Parameter.new("c1", "p11", d)
        s.vhosts["/"].add_parameter(p)
        response = http.get("/api/definitions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "component", "value"]
        body["parameters"].as_a.empty?.should be_false
        body["parameters"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end
  describe "GET /api/definitions/vhost" do
    it "exports queues" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_queue("export_q2", false, false)
        response = http.get("/api/definitions/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["queues"].as_a.empty?.should be_false
        keys = ["name", "vhost", "auto_delete", "durable", "arguments"]
        body["queues"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "does not export exclusive queues" do
      with_http_server do |http, s|
        # Declare exclusive queue with amqp client
        with_channel(s) do |ch|
          ch.queue("", exclusive: true)
          response = http.get("/api/definitions/%2f")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["queues"].as_a.empty?.should be_true
        end
      end
    end

    it "exports exchanges" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("export_e2", "topic", false, false)
        response = http.get("/api/definitions/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "vhost", "auto_delete", "durable", "arguments", "type", "internal"]
        body["exchanges"].as_a.empty?.should be_false
        body["exchanges"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("export_x1", "direct", false, true)
        s.vhosts["/"].declare_queue("export_q1", false, true)
        s.vhosts["/"].bind_queue("export_q1", "export_x1", "", AMQ::Protocol::Table.new)
        response = http.get("/api/definitions/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["source", "vhost", "destination", "destination_type", "routing_key", "arguments"]
        body["bindings"].as_a.empty?.should be_false
        body["bindings"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "exports policies" do
      with_http_server do |http, s|
        d = {"x-max-lenght" => JSON::Any.new(10_i64)}
        s.vhosts["/"].add_policy("export_p2", "^.*", "queues", d, -1_i8)
        response = http.get("/api/definitions/%2f")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        keys = ["name", "vhost", "pattern", "apply-to", "definition", "priority"]
        body["policies"].as_a.empty?.should be_false
        body["policies"].as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
    describe "user tags and vhost access" do
      it "export vhost definitions as management user" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
          s.vhosts.create("new")
          s.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
          headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
          response = http.get("/api/definitions/new", headers: headers)
          response.status_code.should eq 200
        end
      end

      it "should refuse vhost access" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
          s.vhosts.create("new")
          headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
          response = http.get("/api/definitions/new", headers: headers)
          response.status_code.should eq 403
          body = JSON.parse(response.body)
          body["reason"].should eq "Access refused"
        end
      end
    end
  end
  describe "POST /api/definitions/vhost" do
    it "imports queues" do
      with_http_server do |http, s|
        body = %({ "queues": [{ "name": "import_q1", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }] })
        response = http.post("/api/definitions/%2f", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].queues.has_key?("import_q1").should be_true
      end
    end

    it "imports exchanges" do
      with_http_server do |http, s|
        body = %({ "exchanges": [{ "name": "import_x1", "type": "direct", "vhost": "/", "durable": true, "internal": false, "auto_delete": false, "arguments": {} }] })
        response = http.post("/api/definitions/%2f", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].exchanges.has_key?("import_x1").should be_true
      end
    end

    it "imports bindings" do
      with_http_server do |http, s|
        s.vhosts["/"].declare_exchange("import_x1", "direct", false, true)
        s.vhosts["/"].declare_exchange("import_x2", "fanout", false, true)
        s.vhosts["/"].declare_queue("import_q1", false, true)
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
        response = http.post("/api/definitions/%2f", body: body)
        response.status_code.should eq 200
        ex = s.vhosts["/"].exchanges["import_x1"]
        qs = Set(LavinMQ::Queue).new
        es = Set(LavinMQ::Exchange).new
        ex.find_queues("r.k2", nil, qs, es)
        res = Set(LavinMQ::Exchange).new
        res << s.vhosts["/"].exchanges["import_x1"]
        res << s.vhosts["/"].exchanges["import_x2"]
        es.should eq res
        qs = Set(LavinMQ::Queue).new
        es = Set(LavinMQ::Exchange).new
        ex.find_queues("rk", nil, qs, es)
        res = Set(LavinMQ::Queue).new
        res << s.vhosts["/"].queues["import_q1"]
        qs.should eq res
      end
    end

    it "imports policies" do
      with_http_server do |http, s|
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
        response = http.post("/api/definitions/%2f", body: body)
        response.status_code.should eq 200
        s.vhosts["/"].policies.has_key?("import_p1").should be_true
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.post("/api/definitions/%2f", body: "")
        response.status_code.should eq 200
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.post("/api/definitions/%2f", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.post("/api/definitions/%2f", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
    describe "user tags and vhost access" do
      it "import vhost definitions as policymaker user" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::PolicyMaker], save: false) # Will be the new default_user
          s.vhosts.create("new")
          s.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
          headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
          body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
          response = http.post("/api/definitions/new", headers: headers, body: body)
          response.status_code.should eq 200
          s.vhosts["new"].queues.has_key?("import_q1").should be_true
        end
      end

      it "should refuse vhost access" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
          s.vhosts.create("new")
          headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
          body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
          response = http.post("/api/definitions/new", headers: headers, body: body)
          response.status_code.should eq 403
          body = JSON.parse(response.body)
          body["reason"].should eq "Access refused"
        end
      end

      it "should refuse user tag access" do
        with_http_server do |http, s|
          s.users.delete("guest")
          s.users.create("other_name", "guest", [LavinMQ::Tag::Management], save: false) # Will be the new default_user
          s.vhosts.create("new")
          s.users.add_permission("other_name", "new", /.*/, /.*/, /.*/)
          headers = HTTP::Headers{"Authorization" => "Basic b3RoZXJfbmFtZTpndWVzdA=="}
          body = %({ "queues": [{ "name": "import_q1", "vhost": "new", "durable": true, "auto_delete": false, "arguments": {} }] })
          response = http.post("/api/definitions/new", headers: headers, body: body)
          response.status_code.should eq 403
          body = JSON.parse(response.body)
          body["reason"].should eq "Access refused"
        end
      end
    end
  end
  describe "POST /api/definitions/upload" do
    it "imports definitions from uploaded file (no Referer)" do
      with_http_server do |http, s|
        file_content = %({ "vhosts":[{ "name":"uploaded_vhost" }] }) # sanity check
        io = IO::Memory.new
        builder = HTTP::FormData::Builder.new(io)
        builder.file("file", IO::Memory.new(file_content))
        builder.finish

        headers = {"Content-Type" => builder.content_type}
        body = io.to_s

        response = http.post("/api/definitions/upload", headers: headers, body: body)
        response.status_code.should eq 200
        s.vhosts["uploaded_vhost"]?.should_not be_nil
        s.vhosts["uploaded_vhost"].should be_a(LavinMQ::VHost)
      end
    end

    it "imports definitions from uploaded file" do
      with_http_server do |http, s|
        file_content = %({ "vhosts":[{ "name":"uploaded_vhost" }] }) # sanity check
        io = IO::Memory.new
        builder = HTTP::FormData::Builder.new(io)
        builder.file("file", IO::Memory.new(file_content))
        builder.finish

        headers = {"Content-Type" => builder.content_type, "Referer" => "/foo"}
        body = io.to_s

        response = http.post("/api/definitions/upload", headers: headers, body: body)
        response.status_code.should eq 302
        response.headers["Location"].should eq "/foo"
        s.vhosts["uploaded_vhost"]?.should_not be_nil
        s.vhosts["uploaded_vhost"].should be_a(LavinMQ::VHost)
      end
    end

    it "imports definitions from json body" do
      with_http_server do |http, s|
        body = {vhosts: [{name: "uploaded_vhost"}]}.to_json
        headers = {"Content-Type" => "application/json"}
        response = http.post("/api/definitions/upload", headers: headers, body: body)
        response.status_code.should eq 200
        s.vhosts["uploaded_vhost"]?.should_not be_nil
        s.vhosts["uploaded_vhost"].should be_a(LavinMQ::VHost)
      end
    end
  end

  it "should update existing user on import" do
    with_http_server do |http, s|
      name = "bcryptuser"
      body = %({
      "users":[{
        "name":"#{name}",
        "password_hash":"$2a$04$g5IMwYwvgDLACYdAQxCpCulKuK/Ym2I56Tz6T9Wi9DGdKQG.DE8Gi",
        "hashing_algorithm":"Bcrypt","tags":""
      }]
    })

      response = http.post("/api/definitions", body: body)
      response.status_code.should eq 200

      u = s.users[name]
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
      response = http.post("/api/definitions", body: update_body)
      response.status_code.should eq 200

      u = s.users[name]
      u.should be_a(LavinMQ::User)
      ok = u.not_nil!.password.not_nil!.verify "test"
      {u.name, ok}.should eq({name, true})
    end
  end

  it "shouldn't ruin internal state of delayed exchange (issue #698)" do
    with_http_server do |http, s|
      vhost = s.vhosts["/"]
      args = {"x-delayed-message", false, false, false, LavinMQ::AMQP::Table.new({"x-delayed-type": "direct"})}
      vhost.declare_exchange "test", *args
      http.get("/api/definitions")
      vhost.exchanges["test"].match?(*args).should be_true
    end
  end

  it "should be able to import delayed exchanges created in LavinMQ (issue #743)" do
    with_http_server do |http, s|
      body = %({
        "type": "direct",
        "durable": true,
        "internal": false,
        "auto_delete": false,
        "delayed": true
      })
      http.put("/api/exchanges/%2f/test-delayed", body: body)
      response = http.get("/api/definitions")
      body = JSON.parse(response.body)
      http.delete("/api/exchanges/%2f/test-delayed")
      LavinMQ::HTTP::DefinitionsController::GlobalDefinitions.new(s).import(body)
      response = http.get("/api/exchanges/%2f/test-delayed")
      response.status_code.should eq 200
    end
  end
end
