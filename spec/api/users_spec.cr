require "../spec_helper"

describe LavinMQ::HTTP::UsersController do
  describe "GET /api/users" do
    it "should return all users" do
      with_http_server do |http, _|
        response = http.get("/api/users")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "password_hash", "hashing_algorithm"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "should refuse non administrators" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/users", headers: hdrs)
        response.status_code.should eq 403
      end
    end
  end
  describe "GET /api/users/without-permissions" do
    it "should return users without access to any vhost" do
      with_http_server do |http, s|
        s.users.create("alan", "alan")
        response = http.get("/api/users/without-permissions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
  describe "POST /api/users/bulk-delete" do
    it "should delete users in bulk" do
      with_http_server do |http, s|
        s.users.create("alan1", "alan")
        s.users.create("alan2", "alan")
        body = <<-JSON
          {
            "users": ["alan1", "alan2"]
          }
          JSON
        response = http.post("/api/users/bulk-delete", body: body)
        response.status_code.should eq 204
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.post("/api/users/bulk-delete", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field .+ is required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.post("/api/users/bulk-delete", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.post("/api/users/bulk-delete", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
  end
  describe "GET /api/users/name" do
    it "should return user" do
      with_http_server do |http, s|
        s.users.create("alan", "alan")
        response = http.get("/api/users/alan")
        response.status_code.should eq 200
      end
    end
  end
  describe "PUT /api/users/name" do
    it "should create user with password" do
      with_http_server do |http, s|
        body = <<-JSON
          {
            "password": "test"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        u = s.users["alan"]
        ok = u.not_nil!.password.try &.verify("test")
        ok.should be_true
      end
    end

    it "should create user with password_hash" do
      with_http_server do |http, s|
        body = <<-JSON
          {
            "password_hash": "kI3GCqW5JLMJa4iX1lo7X4D6XbYqlLgxIs30+P6tENUV2POR"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        u = s.users["alan"]
        u.not_nil!.password.not_nil!.verify("test12").should be_true
      end
    end

    it "should return 400 when password_hash is null" do
      with_http_server do |http, _|
        response = http.put("/api/users/alan", body: %({"password_hash": null}))
        response.status_code.should eq 400
      end
    end

    it "should return 400 when password_hash is a non-string type" do
      with_http_server do |http, _|
        response = http.put("/api/users/alan", body: %({"password_hash": 123}))
        response.status_code.should eq 400
      end
    end

    it "should return 400 for an unsupported hashing_algorithm" do
      with_http_server do |http, _|
        body = %({"password_hash": "abc", "hashing_algorithm": "rabbit_password_hashing_xyz"})
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 400
      end
    end

    it "should return 400 for an empty hashing_algorithm" do
      with_http_server do |http, _|
        body = %({"password_hash": "abc", "hashing_algorithm": ""})
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 400
      end
    end

    it "should create user with empty password_hash" do
      with_http_server do |http, _|
        body = <<-JSON
          {
            "password_hash": ""
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        hrds = HTTP::Headers{"Authorization" => "Basic YWxhbjo="} # alan:
        response = http.get("/api/users/alan", headers: hrds)
        response.status_code.should eq 401
      end
    end

    it "should expose null hashing_algorithm for passwordless user" do
      with_http_server do |http, _|
        body = %({"password_hash": ""})
        http.put("/api/users/alan", body: body)
        response = http.get("/api/users/alan")
        response.status_code.should eq 200
        parsed = JSON.parse(response.body)
        parsed["password_hash"].as_s.should eq ""
        parsed["hashing_algorithm"].raw.should be_nil
      end
    end

    it "should create user with uniq tags" do
      with_http_server do |http, s|
        body = <<-JSON
          {
            "password": "test",
            "tags": "management,management"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        s.users["alan"].tags.size.should eq 1
        s.users["alan"].tags.should eq([LavinMQ::Tag::Management])
      end
    end

    it "should update user" do
      with_http_server do |http, s|
        s.users.create("alan", "pw")
        body = <<-JSON
          {
            "password": "test",
            "tags": "management"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 204
        s.users["alan"].tags.should eq([LavinMQ::Tag::Management])
      end
    end

    it "should update user with uniq tags" do
      with_http_server do |http, s|
        s.users.create("alan", "pw")
        body = <<-JSON
          {
            "password": "test",
            "tags": "management,management"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 204
        s.users["alan"].tags.size.should eq 1
        s.users["alan"].tags.should eq([LavinMQ::Tag::Management])
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.put("/api/users/alice", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Field .+ is required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/users/alice", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/users/alice", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end

    it "should not create user if disk is full" do
      with_http_server do |http, s|
        s.flow(false)
        body = <<-JSON
          {
            "password": "test"
          }
          JSON
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 412
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Server low on disk space, can not create new user")
      ensure
        s.flow(true)
      end
    end
  end

  describe "GET /api/users/user/permissions" do
    it "should return permissions for user" do
      with_http_server do |http, _|
        response = http.get("/api/users/guest/permissions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["user", "vhost", "configure", "write", "read"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end

  describe "PUT /api/auth/hash_password" do
    it "should return hashed password" do
      with_http_server do |http, _s|
        body = <<-JSON
          {
            "password": "a_pasword_to_hash"
          }
          JSON
        response = http.put("/api/auth/hash_password", body: body)
        response.status_code.should eq 200
        JSON.parse(response.body)["password_hash"].as_s.size.should eq 48
      end
    end
  end
end

describe "vhost scoped users API" do
  describe "PUT /api/vhosts/:vhost/users/:name" do
    it "creates a user scoped to the vhost with full permissions by default" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        response = http.put("/api/vhosts/tenant/users/alice", body: %({"password":"pw","tags":"management"}))
        response.status_code.should eq 201
        u = s.users["alice", "tenant"]
        u.vhost.should eq "tenant"
        u.tags.should eq [LavinMQ::Tag::Management]
        u.permissions["tenant"].should eq({config: /.*/, read: /.*/, write: /.*/})
        s.users["alice"]?.should be_nil
        # persisted in the vhost's own directory, not in the global users.json
        vhost_file = File.join(s.vhosts["tenant"].data_dir, "users.json")
        JSON.parse(File.read(vhost_file)).as_a.map(&.["name"].as_s).should eq ["alice"]
        JSON.parse(File.read(File.join(LavinMQ::Config.instance.data_dir, "users.json"))).as_a
          .any? { |x| x["name"] == "alice" }.should be_false
      end
    end

    it "accepts permission fields on creation" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        body = %({"password":"pw","configure":"^a","read":"^b","write":"^c"})
        response = http.put("/api/vhosts/tenant/users/alice", body: body)
        response.status_code.should eq 201
        s.users["alice", "tenant"].permissions["tenant"].should eq({config: /^a/, read: /^b/, write: /^c/})
      end
    end

    it "rejects invalid permission regexes" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        body = {"password" => "pw", "configure" => "(", "read" => ".*", "write" => ".*"}.to_json
        response = http.put("/api/vhosts/tenant/users/alice", body: body)
        response.status_code.should eq 400
        s.users["alice", "tenant"]?.should be_nil
      end
    end

    it "updates an existing scoped user" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw", vhost: "tenant")
        response = http.put("/api/vhosts/tenant/users/alice", body: %({"password":"new","tags":"monitoring"}))
        response.status_code.should eq 204
        u = s.users["alice", "tenant"]
        u.password.not_nil!.verify("new").should be_true
        u.tags.should eq [LavinMQ::Tag::Monitoring]
      end
    end

    it "returns 404 for unknown vhosts" do
      with_http_server do |http, _|
        response = http.put("/api/vhosts/nope/users/alice", body: %({"password":"pw"}))
        response.status_code.should eq 404
      end
    end

    it "refuses non administrators" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.put("/api/vhosts/tenant/users/alice", headers: hdrs, body: %({"password":"pw"}))
        response.status_code.should eq 403
      end
    end
  end

  describe "GET /api/vhosts/:vhost/users" do
    it "lists only users scoped to the vhost" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.vhosts.create("other")
        s.users.create("alice", "pw", vhost: "tenant")
        s.users.create("bob", "pw", vhost: "other")
        response = http.get("/api/vhosts/tenant/users")
        response.status_code.should eq 200
        body = JSON.parse(response.body).as_a
        body.map(&.["name"].as_s).should eq ["alice"]
        body.first["vhost"].as_s.should eq "tenant"
      end
    end

    it "includes scoped users in the global user listing with their vhost" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw", vhost: "tenant")
        response = http.get("/api/users")
        body = JSON.parse(response.body).as_a
        scoped = body.find! { |u| u["name"] == "alice" }
        scoped["vhost"].as_s.should eq "tenant"
        body.find! { |u| u["name"] == "guest" }["vhost"].raw.should be_nil
      end
    end
  end

  describe "GET /api/vhosts/:vhost/users/:name" do
    it "returns the scoped user and not a global user with the same name" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw", [LavinMQ::Tag::Administrator])
        s.users.create("alice", "pw", [LavinMQ::Tag::Monitoring], vhost: "tenant")
        response = http.get("/api/vhosts/tenant/users/alice")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body["vhost"].as_s.should eq "tenant"
        body["tags"].as_s.should eq "monitoring"
        http.get("/api/vhosts/tenant/users/nobody").status_code.should eq 404
      end
    end
  end

  describe "DELETE /api/vhosts/:vhost/users/:name" do
    it "deletes the scoped user only" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw")
        s.users.create("alice", "pw", vhost: "tenant")
        response = http.delete("/api/vhosts/tenant/users/alice")
        response.status_code.should eq 204
        s.users["alice", "tenant"]?.should be_nil
        s.users["alice"]?.should_not be_nil
        http.delete("/api/vhosts/tenant/users/alice").status_code.should eq 404
      end
    end
  end

  describe "/api/vhosts/:vhost/users/:name/permissions" do
    it "sets, gets and clears permissions of a scoped user" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw", vhost: "tenant")
        http.get("/api/vhosts/tenant/users/alice/permissions").status_code.should eq 404

        body = %({"configure":"^a","read":"^b","write":"^c"})
        http.put("/api/vhosts/tenant/users/alice/permissions", body: body).status_code.should eq 201
        http.put("/api/vhosts/tenant/users/alice/permissions", body: body).status_code.should eq 204

        response = http.get("/api/vhosts/tenant/users/alice/permissions")
        response.status_code.should eq 200
        perm = JSON.parse(response.body)
        perm["user"].as_s.should eq "alice"
        perm["vhost"].as_s.should eq "tenant"
        perm["configure"].as_s.should eq "^a"
        perm["vhost_scoped"].as_bool.should be_true

        http.delete("/api/vhosts/tenant/users/alice/permissions").status_code.should eq 204
        s.users["alice", "tenant"].permissions.should be_empty
      end
    end

    it "flags scoped permissions in the global permissions listing" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        u = s.users.create("alice", "pw", vhost: "tenant")
        s.users.add_permission(u, "tenant", /.*/, /.*/, /.*/)
        response = http.get("/api/permissions")
        perms = JSON.parse(response.body).as_a
        scoped = perms.find! { |p| p["user"] == "alice" }
        scoped["vhost_scoped"].as_bool.should be_true
        perms.find! { |p| p["user"] == "guest" }["vhost_scoped"]?.should be_nil
      end
    end
  end

  describe "DELETE /api/vhosts/:vhost" do
    it "deletes the users scoped to the vhost" do
      with_http_server do |http, s|
        s.vhosts.create("tenant")
        s.users.create("alice", "pw", vhost: "tenant")
        http.delete("/api/vhosts/tenant").status_code.should eq 204
        s.users["alice", "tenant"]?.should be_nil
        s.users.scoped_users("tenant").should be_empty
      end
    end
  end
end
