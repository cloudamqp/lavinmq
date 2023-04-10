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
      Server.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("/api/permissions", headers: hdrs)
      response.status_code.should eq 403
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
      Server.users.create("test_user", "pw")
      Server.vhosts.create("test_vhost")
      body = %({
        "configure": ".*",
        "read": ".*",
        "write": ".*"
      })
      response = put("/api/permissions/test_vhost/test_user", body: body)
      response.status_code.should eq 201
      Server.users["test_user"].permissions["test_vhost"].should eq({config: /.*/, read: /.*/, write: /.*/})
    end

    it "should update permission for a user and vhost" do
      Server.vhosts.create("test_vhost")
      Server.users.add_permission("guest", "test_vhost", /.*/, /.*/, /.*/)

      body = %({
        "configure": ".*",
        "read": ".*",
        "write": "^tut"
      })
      response = put("/api/permissions/test_vhost/guest", body: body)
      response.status_code.should eq 204
      Server.users["guest"].permissions["test_vhost"]["write"].should eq(/^tut/)
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
      Server.vhosts.create("test")
      Server.users.add_permission("guest", "test", /.*/, /.*/, /.*/)
      response = delete("/api/permissions/test/guest")
      response.status_code.should eq 204
    end
  end
end
