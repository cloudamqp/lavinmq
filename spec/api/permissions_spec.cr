require "../spec_helper"

describe LavinMQ::HTTP::PermissionsController do
  describe "GET /api/permissions" do
    it "should return all permissions" do
      response = get("/api/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "read", "write"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "should refuse non administrators" do
      s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/permissions", headers: hdrs)
      response.status_code.should eq 401
    ensure
      s.users.delete("arnold")
    end
  end

  describe "GET /api/permissions/vhost/user" do
    it "should return all permissions for a user and vhost" do
      response = get("/api/permissions/%2f/guest")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_h.empty?.should be_false
    end
  end

  describe "PUT /api/permissions/vhost/user" do
    it "should create permission for a user and vhost" do
      s.users.create("test_user", "pw")
      s.vhosts.create("test_vhost")
      body = %({
        "configure": ".*",
        "read": ".*",
        "write": ".*"
      })
      response = put("/api/permissions/test_vhost/test_user", body: body)
      response.status_code.should eq 201
      s.users["test_user"].permissions["test_vhost"].should eq({config: /.*/, read: /.*/, write: /.*/})
    ensure
      s.users.rm_permission("test_user", "test_vhost")
      s.vhosts.delete("test_vhost")
    end

    it "should update permission for a user and vhost" do
      s.vhosts.create("test_vhost")
      s.users.add_permission("guest", "test_vhost", /.*/, /.*/, /.*/)

      body = %({
        "configure": ".*",
        "read": ".*",
        "write": "^tut"
      })
      response = put("/api/permissions/test_vhost/guest", body: body)
      response.status_code.should eq 204
      s.users["guest"].permissions["test_vhost"]["write"].should eq(/^tut/)
    ensure
      s.users.rm_permission("guest", "test_vhost")
      s.vhosts.delete("test_vhost")
    end

    it "should handle request with empty body" do
      response = put("/api/permissions/%2f/guest", body: "")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should match(/Fields .+ are required/)
    end

    it "should handle unexpected input" do
      response = put("/api/permissions/%2f/guest", body: "\"{}\"")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Input needs to be a JSON object.")
    end

    it "should handle invalid JSON" do
      response = put("/api/permissions/%2f/guest", body: "a")
      response.status_code.should eq 400
      body = JSON.parse(response.body)
      body["reason"].as_s.should eq("Malformed JSON.")
    end
  end

  describe "DELETE /api/permissions/vhost/user" do
    it "should delete permission for a user and vhost" do
      s.vhosts.create("test")
      s.users.add_permission("guest", "test", /.*/, /.*/, /.*/)
      response = delete("/api/permissions/test/guest")
      response.status_code.should eq 204
    ensure
      s.vhosts.delete("test")
    end
  end
end
