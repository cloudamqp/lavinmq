require "../spec_helper"

describe LavinMQ::HTTP::VHostsController do
  describe "GET /api/vhosts" do
    it "should return all vhosts" do
      response = get("/api/vhosts")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["name", "dir", "messages", "messages_unacknowledged", "messages_ready"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "only return vhosts user has access to if mangement tag" do
      Server.users.create("arnold", "pw", [LavinMQ::Tag::Management])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/vhosts", headers: hdrs)
      response.status_code.should eq 200
      JSON.parse(response.body).as_a.size.should eq 0
    end

    it "should list vhosts for monitoring users" do
      Server.users.create("arnold", "pw", [LavinMQ::Tag::Monitoring])
      Server.vhosts.create("test")
      Server.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/vhosts", headers: hdrs)
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.any? { |v| v["name"].as_s == "/" }.should be_true
      body.as_a.any? { |v| v["name"].as_s == "test" }.should be_true
    end
  end

  describe "GET /api/vhosts/vhost" do
    it "should return vhost" do
      response = get("/api/vhosts/%2f")
      response.status_code.should eq 200
    end

    it "should return 404 if vhost does not exist" do
      response = get("/api/vhosts/404")
      response.status_code.should eq 404
    end
  end

  describe "PUT /api/vhosts/vhost" do
    it "should create vhost" do
      response = put("/api/vhosts/test")
      response.status_code.should eq 201
    end

    it "should update vhost" do
      Server.vhosts.create("test")
      response = put("/api/vhosts/test")
      response.status_code.should eq 204
    end

    it "should only allow administrators to create vhost" do
      Server.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = put("/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 403
    end

    it "should create a vhost with full permissions to user" do
      vhost = "test-vhost"
      username = "arnold"

      user = Server.users.create(username, "pw", [LavinMQ::Tag::Administrator])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = put("/api/vhosts/#{vhost}", headers: hdrs)
      response.status_code.should eq 201
      p = user.permissions[vhost]
      p[:config].should eq /.*/
      p[:read].should eq /.*/
      p[:write].should eq /.*/
    end
  end

  describe "DELETE /api/vhosts/vhost" do
    it "should delete vhost" do
      Server.vhosts.create("test")
      response = delete("/api/vhosts/test")
      response.status_code.should eq 204
    end

    it "should only allow administrators to delete vhost" do
      Server.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = delete("/api/vhosts/test", headers: hdrs)
      response.status_code.should eq 403
    end
  end

  describe "GET /api/vhosts/vhost/permissions" do
    it "should return permissions for vhosts" do
      response = get("/api/vhosts/%2f/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end
end
