require "../spec_helper"

describe LavinMQ::HTTP::ConnectionsController do
  describe "GET /api/connections" do
    it "should return network connections" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/connections")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
        end
      end
    end

    it "should only show own connections for policymaker" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        with_channel(s) do
          response = http.get("/api/connections", headers: hdrs)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_true
        end
      end
    end

    it "should show all connections for monitoring" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Monitoring])
        hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        with_channel(s) do
          response = http.get("/api/connections", headers: hdrs)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
        end
      end
    end
  end

  describe "GET /api/vhosts/vhost/connections" do
    it "should return network connections for vhost" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/vhosts/%2f/connections")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
          keys = ["channels", "connected_at", "type", "channel_max", "timeout", "client_properties",
                  "vhost", "user", "protocol", "auth_mechanism", "host", "port", "name", "ssl",
                  "state"]
          val = body.as_a.last
          keys.each { |k| val.as_h.keys.should contain(k) }
        end
      end
    end

    it "should return 404 if vhosts does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/vhosts/vhost/connections")
        response.status_code.should eq 404
      end
    end
  end
  describe "GET /api/connections/name" do
    it "should return connection" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/vhosts/%2f/connections")
          body = JSON.parse(response.body)
          name = URI.encode_www_form(body.as_a.last["name"].as_s)
          response = http.get("/api/connections/#{name}")
          response.status_code.should eq 200
        end
      end
    end

    it "should return 404 if connection does not exist" do
      with_http_server do |http, _|
        response = http.get("/api/connections/name")
        response.status_code.should eq 404
      end
    end

    it "should return 403 if user doesn't have access" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::PolicyMaker])
        hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        with_channel(s) do
          response = http.get("/api/vhosts/%2f/connections")
          body = JSON.parse(response.body)
          name = URI.encode_www_form(body.as_a.last["name"].as_s)
          response = http.get("/api/connections/#{name}", headers: hdrs)
          response.status_code.should eq 403
        end
      end
    end
  end

  describe "GET /api/connections/name/channels" do
    it "should return channels for a connection" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/vhosts/%2f/connections")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          name = URI.encode_www_form(body.as_a.last["name"].as_s)
          response = http.get("/api/connections/#{name}/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.size.should eq 1
        end
      end
    end
  end

  describe "GET /api/connections/username/:username" do
    it "returns connections for a specific user" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Administrator])
        hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        with_channel(s) do
          response = http.get("/api/connections/username/arnold", headers: hdrs)
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_true
        end
      end
    end
  end

  describe "DELETE /api/connections/username/:username" do
    it "deletes connections for a specific user" do
      with_http_server do |http, s|
        s.users.create("arnold", "pw", [LavinMQ::Tag::Administrator])
        s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
        hdrs = HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
        with_channel(s, user: "arnold", password: "pw") do
          response = http.delete("/api/connections/username/arnold", headers: hdrs)
          response.status_code.should eq 204
          sleep 0.1.seconds
          response = http.get("/api/connections/username/arnold", headers: hdrs)
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_true
        end
      end
    end
  end
end
