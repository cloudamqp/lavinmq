require "../spec_helper"

describe AvalancheMQ::UsersController do
  describe "GET /api/users" do
    it "should return all users" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/users")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "password_hash", "hashing_algorithm"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end

    it "should refuse non administrators" do
      s, h = create_servers
      listen(h)
      s.try &.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/users", headers: hdrs)
      response.status_code.should eq 401
    ensure
      close(h)
    end
  end

  describe "GET /api/users/without-permissions" do
    it "should return users without access to any vhost" do
      s, h = create_servers
      listen(h)
      s.users.create("alan", "alan")
      response = get("http://localhost:8080/api/users/without-permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end

  describe "POST /api/users/bulk-delete" do
    it "should delete users in bulk" do
      s, h = create_servers
      listen(h)
      s.users.create("alan1", "alan")
      s.users.create("alan2", "alan")
      body = %({
        "users": ["alan1", "alan2"]
      })
      response = post("http://localhost:8080/api/users/bulk-delete", body: body)
      response.status_code.should eq 204
    ensure
      close(h)
    end
  end

  describe "GET /api/users/name" do
    it "should return user" do
      s, h = create_servers
      listen(h)
      s.users.create("alan", "alan")
      response = get("http://localhost:8080/api/users/alan")
      response.status_code.should eq 200
    ensure
      close(h)
    end
  end

  describe "PUT /api/users/name" do
    it "should create user with password" do
      s, h = create_servers
      listen(h)
      s.users.delete("alan")
      body = %({
        "password": "test"
      })
      response = put("http://localhost:8080/api/users/alan", body: body)
      response.status_code.should eq 204
      u = s.users["alan"]
      ok = u.not_nil!.password == "test"
      ok.should be_true
    ensure
      close(h)
    end

    it "should create user with password_hash" do
      s, h = create_servers
      listen(h)
      s.users.delete("alan")
      body = %({
        "password_hash": "kI3GCqW5JLMJa4iX1lo7X4D6XbYqlLgxIs30+P6tENUV2POR"
      })
      response = put("http://localhost:8080/api/users/alan", body: body)
      response.status_code.should eq 204
      u = s.users["alan"]
      ok = u.not_nil!.password == "test12"
      ok.should be_true
    ensure
      close(h)
    end

    it "should create user with empty password_hash" do
      s, h = create_servers
      listen(h)
      s.users.delete("alan")
      body = %({
        "password_hash": ""
      })
      response = put("http://localhost:8080/api/users/alan", body: body)
      response.status_code.should eq 204
      hrds = HTTP::Headers{"Authorization" => "Basic YWxhbjo="} # alan:
      response = get("http://localhost:8080/api/users/alan", headers: hrds)
      response.status_code.should eq 401
    ensure
      close(h)
    end
  end

  describe "GET /api/users/user/permissions" do
    it "should return permissions for user" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/users/guest/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "write", "read"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end
  end
end
