require "../spec_helper"
require "uri"

describe AvalancheMQ::HTTP::ChannelsController do
  describe "GET /api/channels" do
    it "should return all channels" do
      with_channel do
        response = get("/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_false
        keys = ["vhost", "user", "number", "name", "connection_details", "state", "prefetch_count",
                "global_prefetch_count", "consumer_count", "confirm", "transactional"]
        body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
      end
    end
  end

  describe "GET /api/vhosts/vhost/channels" do
    it "should return all channels for a vhost" do
      with_channel do
        response = get("/api/vhosts/%2f/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 1
      end
    end

    it "should return empty array if no connections" do
      s.vhosts.create("no-conns")
      response = get("/api/vhosts/no-conns/channels")
      response.status_code.should eq 200
      body = JSON.parse(response.body)
      body.as_a.empty?.should be_true
    ensure
      s.vhosts.delete("no-conns")
    end
  end

  describe "GET /api/channels/channel" do
    it "should return channel" do
      with_channel do
        response = get("/api/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        name = URI.escape(body[0]["name"].as_s)
        response = get("/api/channels/#{name}")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        expected_keys = ["consumer_details"]
        actual_keys = body.as_h.keys
        expected_keys.each { |k| actual_keys.should contain(k) }
      end
    end
  end
end
