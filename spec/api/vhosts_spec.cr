require "../spec_helper"

describe AvalancheMQ::VHostsController do
  describe "GET /api/vhosts" do
    it "should return all vhosts" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/vhosts")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "dir", "messages", "messages_unacknowledged", "messages_ready"]
      body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    ensure
      close(h)
    end

    it "should require management access" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [] of AvalancheMQ::Tag)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.try &.users.delete("arnold")
      close(h)
    end

    it "should only list vhosts user has access to" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Management])
      s.vhosts.create("test")
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.any? { |v| v["name"].as_s == "/" }.should be_true
      body.any? { |v| v["name"].as_s == "test" }.should be_false
    ensure
      s.try do |s|
        s.vhosts.delete("test")
        s.users.delete("arnold")
      end
      close(h)
    end

    it "should list vhosts from monitoring users" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Monitoring])
      s.vhosts.create("test")
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.any? { |v| v["name"].as_s == "/" }.should be_true
      body.any? { |v| v["name"].as_s == "test" }.should be_true
    ensure
      s.try do |s|
        s.vhosts.delete("test")
        s.users.delete("arnold")
      end
      close(h)
    end
  end

  describe "GET /api/vhosts/vhost" do
    it "should return vhost" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/vhosts/%2f")
      response.status_code.should eq 200
    ensure
      close(h)
    end

    it "should return 404 if vhost does not exist" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/vhosts/404")
      response.status_code.should eq 404
    ensure
      close(h)
    end

    it "should return 401 if user does not have access to vhost" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Management])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts/%2f", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.try &.users.delete("arnold")
      close(h)
    end
  end

  describe "PUT /api/vhosts/vhost" do
    it "should create vhost" do
      s, h = create_servers
      listen(h)
      response = put("http://localhost:8080/api/vhosts/test")
      response.status_code.should eq 201
    ensure
      close(h)
    end

    it "should only allow administrators to create vhost" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = put("http://localhost:8080/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 401
    ensure
      close(h)
    end
  end

  describe "DELETE /api/vhosts/vhost" do
    it "should delete vhost" do
      s, h = create_servers
      listen(h)
      s.vhosts.create("test")
      response = delete("http://localhost:8080/api/vhosts/test")
      response.status_code.should eq 204
    ensure
      close(h)
    end

    it "should only allow administrators to delete vhost" do
      s, h = create_servers
      listen(h)
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = delete("http://localhost:8080/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 401
    ensure
      close(h)
    end
  end

  describe "GET /api/vhosts/vhost/permissions" do
    it "should return permissions for vhosts" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/vhosts/%2f/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    ensure
      close(h)
    end
  end
end
