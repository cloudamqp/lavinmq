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
        body = %({
        "users": ["alan1", "alan2"]
      })
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
        response = http.put("/api/users/bulk-delete", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/users/bulk-delete", body: "a")
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
        body = %({
        "password": "test"
      })
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        u = s.users["alan"]
        ok = u.not_nil!.password.try &.verify("test")
        ok.should be_true
      end
    end

    it "should create user with password_hash" do
      with_http_server do |http, s|
        body = %({
        "password_hash": "kI3GCqW5JLMJa4iX1lo7X4D6XbYqlLgxIs30+P6tENUV2POR"
      })
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        u = s.users["alan"]
        u.not_nil!.password.not_nil!.verify("test12").should be_true
      end
    end

    it "should create user with empty password_hash" do
      with_http_server do |http, _|
        body = %({
        "password_hash": ""
      })
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        hrds = HTTP::Headers{"Authorization" => "Basic YWxhbjo="} # alan:
        response = http.get("/api/users/alan", headers: hrds)
        response.status_code.should eq 401
      end
    end

    it "should create user with uniq tags" do
      with_http_server do |http, s|
        body = %({
        "password": "test",
        "tags": "management,management"
      })
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 201
        s.users["alan"].tags.size.should eq 1
        s.users["alan"].tags.should eq([LavinMQ::Tag::Management])
      end
    end

    it "should update user" do
      with_http_server do |http, s|
        s.users.create("alan", "pw")
        body = %({
        "password": "test",
        "tags": "management"
      })
        response = http.put("/api/users/alan", body: body)
        response.status_code.should eq 204
        s.users["alan"].tags.should eq([LavinMQ::Tag::Management])
      end
    end

    it "should update user with uniq tags" do
      with_http_server do |http, s|
        s.users.create("alan", "pw")
        body = %({
        "password": "test",
        "tags": "management,management"
      })
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
        body["reason"].as_s.should match(/Request body required/)
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
        body = %({
        "password": "test"
      })
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
        body = %({
        "password": "a_pasword_to_hash"
      })
        response = http.put("/api/auth/hash_password", body: body)
        response.status_code.should eq 200
        JSON.parse(response.body)["password_hash"].as_s.size.should eq 48
      end
    end
  end
end
