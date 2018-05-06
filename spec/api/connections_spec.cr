require "../spec_helper"

describe AvalancheMQ::ConnectionsController do
  describe "GET /api/connections" do
    it "should return network connections" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        response = get("http://localhost:8080/api/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
      end
    ensure
      close(h, s)
    end
  end

  describe "GET /api/vhosts/vhost/connections" do
    it "should return network connections" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        response = get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["channels", "connected_at", "type", "channel_max", "timeout", "client_properties",
                "vhost", "user", "protocol", "auth_mechanism", "host", "port", "name", "ssl",
                "state"]
        body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    ensure
      close(h, s)
    end

    it "should return 404 if vhosts does not exist" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/vhosts/vhost/connections")
      response.status_code.should eq 404
    ensure
      close(h)
    end
  end

  describe "GET /api/connections/name" do
    it "should return connection" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        response = get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("http://localhost:8080/api/connections/#{name}")
        response.status_code.should eq 200
      end
    ensure
      close(h, s)
    end

    it "should return 404 if connection does not exist" do
      s, h = create_servers
      listen(h)
      response = get("http://localhost:8080/api/connections/name")
      response.status_code.should eq 404
    ensure
      close(h)
    end
  end

  describe "DELETE /api/connections/name" do
    it "should close connection" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        response = get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = delete("http://localhost:8080/api/connections/#{name}")
      ensure
        response.try &.status_code.should eq 200
      end
    ensure
      close(h, s)
    end
  end

  describe "GET /api/connections/name/channels" do
    it "should return channels for a connection" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = get("http://localhost:8080/api/vhosts/%2f/connections")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("http://localhost:8080/api/connections/#{name}/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    ensure
      close(h, s)
    end
  end
end
