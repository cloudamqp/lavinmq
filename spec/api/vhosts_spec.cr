require "../spec_helper"

describe AvalancheMQ::VHostsController do
  describe "GET /api/vhosts" do
    it "should return all vhosts" do
      response = get("http://localhost:8080/api/vhosts")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "dir", "messages", "messages_unacknowledged", "messages_ready"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "should require management access" do
      s.users.create("arnold", "pw", [] of AvalancheMQ::Tag)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.users.delete("arnold")
    end

    it "should list vhosts from monitoring users" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Monitoring])
      s.vhosts.create("test")
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/vhosts", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.any? { |v| v["name"].as_s == "/" }.should be_true
      body.as_a.any? { |v| v["name"].as_s == "test" }.should be_true
    ensure
      s.try do |s|
        s.vhosts.delete("test")
        s.users.delete("arnold")
      end
    end
  end

  describe "GET /api/vhosts/vhost" do
    it "should return vhost" do
      response = get("http://localhost:8080/api/vhosts/%2f")
      response.status_code.should eq 200
    end

    it "should return 404 if vhost does not exist" do
      response = get("http://localhost:8080/api/vhosts/404")
      response.status_code.should eq 404
    end
  end

  describe "PUT /api/vhosts/vhost" do
    it "should create vhost" do
      response = put("http://localhost:8080/api/vhosts/test")
      response.status_code.should eq 204
    end

    it "should only allow administrators to create vhost" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = put("http://localhost:8080/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 401
    end
  end

  describe "DELETE /api/vhosts/vhost" do
    it "should delete vhost" do
      s.vhosts.create("test")
      response = delete("http://localhost:8080/api/vhosts/test")
      response.status_code.should eq 204
    end

    it "should only allow administrators to delete vhost" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = delete("http://localhost:8080/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 401
    end
  end

  describe "GET /api/vhosts/vhost/permissions" do
    it "should return permissions for vhosts" do
      response = get("http://localhost:8080/api/vhosts/%2f/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end
end
