require "../spec_helper"

describe LavinMQ::HTTP::PermissionsController do
  describe "GET /api/permissions" do
    it "should return all permissions" do
      with_http_server do |http, _|
        response = http.get("/api/permissions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["user", "vhost", "configure", "read", "write"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "should refuse non administrators" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/permissions", headers: hdrs)
        response.status_code.should eq 403
      end
    end
  end
  describe "GET /api/permissions/vhost/user" do
    it "should return all permissions for a user and vhost" do
      with_http_server do |http, _|
        response = http.get("/api/permissions/%2f/guest")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_h.empty?.should be_false
      end
    end
  end

  describe "PUT /api/permissions/vhost/user" do
    it "should create permission for a user and vhost" do
      with_http_server do |http, s|
        s.users.create("test_user", "pw")
        s.vhosts.create("test_vhost")
        body = %({
        "configure": ".*",
        "read": ".*",
        "write": ".*"
      })
        response = http.put("/api/permissions/test_vhost/test_user", body: body)
        response.status_code.should eq 201
        s.users["test_user"].permission?("test_vhost").should eq({config: /.*/, read: /.*/, write: /.*/})
      end
    end

    it "should update permission for a user and vhost" do
      with_http_server do |http, s|
        s.vhosts.create("test_vhost")
        s.users.add_permission("guest", "test_vhost", /.*/, /.*/, /.*/)

        body = %({
        "configure": ".*",
        "read": ".*",
        "write": "^tut"
      })
        response = http.put("/api/permissions/test_vhost/guest", body: body)
        response.status_code.should eq 204
        s.users["guest"].permission?("test_vhost").not_nil![:write].should eq(/^tut/)
      end
    end

    it "should handle request with empty body" do
      with_http_server do |http, _|
        response = http.put("/api/permissions/%2f/guest", body: "")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should match(/Fields .+ are required/)
      end
    end

    it "should handle unexpected input" do
      with_http_server do |http, _|
        response = http.put("/api/permissions/%2f/guest", body: "\"{}\"")
        response.status_code.should eq 400
      end
    end

    it "should handle invalid JSON" do
      with_http_server do |http, _|
        response = http.put("/api/permissions/%2f/guest", body: "a")
        response.status_code.should eq 400
        body = JSON.parse(response.body)
        body["reason"].as_s.should eq("Malformed JSON")
      end
    end
  end

  describe "DELETE /api/permissions/vhost/user" do
    it "should delete permission for a user and vhost" do
      with_http_server do |http, s|
        s.vhosts.create("test")
        s.users.add_permission("guest", "test", /.*/, /.*/, /.*/)
        response = http.delete("/api/permissions/test/guest")
        response.status_code.should eq 204
      end
    end
  end
end
