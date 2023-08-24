require "../spec_helper"

describe LavinMQ::HTTP::UsersController do
  describe "GET /api/users" do
    it "should return all users" do
      response = get("/api/users")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "password_hash", "hashing_algorithm"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "should refuse non administrators" do
      Server.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/users", headers: hdrs)
      response.status_code.should eq 403
    end
  end

  describe "GET /api/users/without-permissions" do
    it "should return users without access to any vhost" do
      Server.users.create("alan", "alan")
      response = get("/api/users/without-permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "POST /api/users/bulk-delete" do
    it "should delete users in bulk" do
      Server.users.create("alan1", "alan")
      Server.users.create("alan2", "alan")
      body = %({
        "users": ["alan1", "alan2"]
      })
      response = post("/api/users/bulk-delete", body: body)
      response.status_code.should eq 204
    end

    it "should handle request with empty body" do
      response = put("/api/users/bulk-delete", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field .+ is required/)
    end

    it "should handle unexpected input" do
      response = put("/api/users/bulk-delete", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Input needs to be a JSON object.")
    end

    it "should handle invalid JSON" do
      response = put("/api/users/bulk-delete", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end
  end

  describe "GET /api/users/name" do
    it "should return user" do
      Server.users.create("alan", "alan")
      response = get("/api/users/alan")
      response.status_code.should eq 200
    end
  end

  describe "PUT /api/users/name" do
    it "should create user with password" do
      body = %({
        "password": "test"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 201
      u = Server.users["alan"]
      ok = u.not_nil!.password.try &.verify("test")
      ok.should be_true
    end

    it "should create user with password_hash" do
      body = %({
        "password_hash": "kI3GCqW5JLMJa4iX1lo7X4D6XbYqlLgxIs30+P6tENUV2POR"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 201
      u = Server.users["alan"]
      u.not_nil!.password.not_nil!.verify("test12").should be_true
    end

    it "should create user with empty password_hash" do
      body = %({
        "password_hash": ""
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 201
      hrds = HTTP::Headers{"Authorization" => "Basic YWxhbjo="} # alan:
      response = get("/api/users/alan", headers: hrds)
      response.status_code.should eq 401
    end

    it "should create user with uniq tags" do
      body = %({
        "password": "test",
        "tags": "management,management"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 201
      Server.users["alan"].tags.size.should eq 1
      Server.users["alan"].tags.should eq([LavinMQ::Tag::Management])
    end

    it "should update user" do
      Server.users.create("alan", "pw")
      body = %({
        "password": "test",
        "tags": "management"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 204
      Server.users["alan"].tags.should eq([LavinMQ::Tag::Management])
    end

    it "should update user with uniq tags" do
      Server.users.create("alan", "pw")
      body = %({
        "password": "test",
        "tags": "management,management"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 204
      Server.users["alan"].tags.size.should eq 1
      Server.users["alan"].tags.should eq([LavinMQ::Tag::Management])
    end

    it "should handle request with empty body" do
      response = put("/api/users/alice", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Field .+ is required/)
    end

    it "should handle unexpected input" do
      response = put("/api/users/alice", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq "Input needs to be a JSON object."
    end

    it "should handle invalid JSON" do
      response = put("/api/users/alice", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end

    it "should not create user if disk is full" do
      Server.flow(false)
      body = %({
        "password": "test"
      })
      response = put("/api/users/alan", body: body)
      response.status_code.should eq 412
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Server low on disk space, can not create new user")
    end
  end

  describe "GET /api/users/user/permissions" do
    it "should return permissions for user" do
      response = get("/api/users/guest/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "write", "read"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end
  end
end
