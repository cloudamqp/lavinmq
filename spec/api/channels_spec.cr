require "../spec_helper"
require "uri"

describe AvalancheMQ::ChannelsController do
  describe "GET /api/channels" do
    it "should return all channels" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = get("http://localhost:8080/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["vhost", "user", "number", "name", "connection_details", "state", "prefetch_count",
                "global_prefetch_count", "consumer_count", "confirm", "transactional"]
        body.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    ensure
      close(h, s)
    end

    it "should return empty array if no connections" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      response = get("http://localhost:8080/api/channels")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      close(h, s)
    end
  end

  describe "GET /api/vhosts/vhost/channels" do
    it "should return all channels for a vhost" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = get("http://localhost:8080/api/vhosts/%2f/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    ensure
      close(h, s)
    end

    it "should return empty array if no connections" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      response = get("http://localhost:8080/api/vhosts/%2f/channels")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      close(h, s)
    end
  end

  describe "GET /api/channels/channel" do
    it "should return channel" do
      s, h = create_servers
      spawn { s.try &.listen(5672) }
      listen(h)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        response = get("http://localhost:8080/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("http://localhost:8080/api/channels/#{name}")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        expected_keys = ["consumer_details"]
        actual_keys = body.as_h.keys
        expected_keys.each { |k| actual_keys.should contain(k) }
      end
    ensure
      close(h, s)
    end
  end
end
