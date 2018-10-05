require "../spec_helper"

describe AvalancheMQ::ConnectionsController do
  describe "GET /api/connections" do
    it "should return network connections" do
      with_channel do
        response = get("/api/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    end

    it "should only show own connections for policymaker" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::PolicyMaker])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      with_channel do
        response = get("/api/connections", headers: hdrs)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    ensure
      s.users.delete("arnold")
    end

    it "should only show all connections for monitoring" do
      s.users.create("arnold", "pw", [AvalancheMQ::Tag::Monitoring])
      hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      with_channel do
        response = get("/api/connections", headers: hdrs)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    ensure
      s.users.delete("arnold")
    end
  end

  describe "GET /api/vhosts/vhost/connections" do
    it "should return network connections" do
      with_channel do
        response = get("/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["channels", "connected_at", "type", "channel_max", "timeout", "client_properties",
                "vhost", "user", "protocol", "auth_mechanism", "host", "port", "name", "ssl",
                "state"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end

    it "should return 404 if vhosts does not exist" do
      response = get("/api/vhosts/vhost/connections")
      response.status_code.should eq 404
    end
  end

  describe "GET /api/connections/name" do
    it "should return connection" do
      with_channel do
        response = get("/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("/api/connections/#{name}")
        response.status_code.should eq 200
      end
    end

    it "should return 404 if connection does not exist" do
      response = get("/api/connections/name")
      response.status_code.should eq 404
    end

    it "should return 401 if user doesn't have access" do
      with_channel do
        response = get("/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = delete("/api/connections/#{name}")
      ensure
        response.try &.status_code.should eq 204
      end
    end
  end

  describe "GET /api/connections/name/channels" do
    it "should return channels for a connection" do
      with_channel do |ch|
        response = get("/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("/api/connections/#{name}/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    end
  end
end
