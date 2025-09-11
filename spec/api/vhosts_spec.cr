require "../spec_helper"

describe LavinMQ::HTTP::VHostsController do
  describe "GET /api/vhosts" do
    it "should return all vhosts" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["name", "dir", "messages", "messages_unacknowledged", "messages_ready"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "only return vhosts user has access to if mangement tag" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/vhosts", headers: hdrs)
        response.status_code.should eq 200
        JSON.parse(response.body).as_a.size.should eq 0
      end
    end

    it "should list vhosts for monitoring users" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Monitoring])
        s.vhosts.create("test")
        s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.get("/api/vhosts", headers: hdrs)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.any? { |v| v["name"].as_s == "/" }.should be_true
        body.as_a.any? { |v| v["name"].as_s == "test" }.should be_true
      end
    end
  end

  describe "GET /api/vhosts/:vhost" do
    it "should return vhost" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts/%2f")
        response.status_code.should eq 200
      end
    end

    it "should return 404 if vhost does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts/404")
        response.status_code.should eq 404
      end
    end
  end

  describe "PUT /api/vhosts/vhost" do
    it "should create vhost" do
      with_http_server do |http, _|
        response = http.put("/api/vhosts/test")
        response.status_code.should eq 201
      end
    end

    it "should update vhost" do
      with_http_server do |http, s|
        s.vhosts.create("test")
        response = http.put("/api/vhosts/test")
        response.status_code.should eq 204
      end
    end

    it "should only allow administrators to create vhost" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.put("/api/vhosts/test", headers: hdrs)
        response.status_code.should eq 403
      end
    end

    it "should create a vhost with full permissions to user" do
      with_http_server do |http, s|
        vhost = "test-vhost"
        username = "arnold"

        user = s.users.create(username, "pw", [LavinMQ::Tag::Administrator])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.put("/api/vhosts/#{vhost}", headers: hdrs)
        response.status_code.should eq 201
        p = user.permission?(vhost).not_nil!
        p[:config].should eq /.*/
        p[:read].should eq /.*/
        p[:write].should eq /.*/
      end
    end
  end

  describe "DELETE /api/vhosts/:vhost" do
    it "should return 204 when deleting vhost" do
      with_http_server do |http, s|
        s.vhosts.create("test")
        response = http.delete("/api/vhosts/test")
        response.status_code.should eq 204
      end
    end

    it "should return 404 when trying to delete non existing vhost" do
      with_http_server do |http, _|
        response = http.delete("/api/vhosts/nonexisting")
        response.status_code.should eq 404
      end
    end

    it "should only allow administrators to delete vhost" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        response = http.delete("/api/vhosts/test", headers: hdrs)
        response.status_code.should eq 403
      end
    end
  end

  describe "GET /api/vhosts/vhost/permissions" do
    it "should return permissions for vhosts" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts/%2f/permissions")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end
  end
end
