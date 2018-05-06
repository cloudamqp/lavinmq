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
    ensure
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
