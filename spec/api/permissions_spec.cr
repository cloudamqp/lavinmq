require "../spec_helper"

describe AvalancheMQ::PermissionsController do
  describe "GET /api/permissions" do
    it "should return all permissions" do
      response = get("http://localhost:8080/api/permissions")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
      keys = ["user", "vhost", "configure", "read", "write"]
      body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
    end

    it "should refuse non administrators" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      response = get("http://localhost:8080/api/permissions", headers: hdrs)
      response.status_code.should eq 401
    end
  end

  describe "GET /api/permissions/vhost/user" do
    it "should return all permissions for a user and vhost" do
      response = get("http://localhost:8080/api/permissions/%2f/guest")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_false
    end
  end

  describe "PUT /api/permissions/vhost/user" do
    it "should create permission for a user and vhost" do
      s.vhosts.create("test")
      body = %({
        "configure": ".*",
        "read": ".*",
        "write": ".*"
      })
      response = put("http://localhost:8080/api/permissions/test/guest", body: body)
      response.status_code.should eq 204
    end
  end

  describe "DELETE /api/permissions/vhost/user" do
    it "should delete permission for a user and vhost" do
      s.vhosts.create("test")
      s.users.add_permission("guest", "test", /.*/, /.*/, /.*/)
      response = delete("http://localhost:8080/api/permissions/test/guest")
      response.status_code.should eq 204
    end
  end
end
