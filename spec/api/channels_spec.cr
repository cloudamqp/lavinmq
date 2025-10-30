require "../spec_helper"
require "uri"

describe LavinMQ::HTTP::ChannelsController do
  describe "GET /api/channels" do
    it "should return all channels" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.empty?.should be_false
          keys = ["vhost", "user", "number", "name", "connection_details", "state", "prefetch_count",
                  "global_prefetch_count", "consumer_count", "confirm", "transactional"]
          body.as_a.each { |v| keys.each { |k| v.as_h.keys.should contain(k) } }
        end
      end
    end
  end

  describe "GET /api/vhosts/vhost/channels" do
    it "should return all channels for a vhost" do
      with_http_server do |http, s|
        s.vhosts.create("my-connection")
        s.users.add_permission("guest", "my-connection", /.*/, /.*/, /.*/)
        with_channel(s, vhost: "my-connection") do
          response = http.get("/api/vhosts/my-connection/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.size.should eq 1
        end
      end
    end

    it "should return empty array if no connections" do
      with_http_server do |http, s|
        s.vhosts.create("no-conns")
        response = http.get("/api/vhosts/no-conns/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.empty?.should be_true
      end
    end

    # Filtering by vhost on the channels page only shows channels for the first connection
    # https://github.com/cloudamqp/lavinmq/issues/1414
    it "should return all channels for a vhost with multiple connections" do
      with_http_server do |http, s|
        s.vhosts.create("my-connection")
        s.users.add_permission("guest", "my-connection", /.*/, /.*/, /.*/)
        wg = WaitGroup.new(1)
        2.times { spawn { with_channel(s, vhost: "my-connection") { wg.wait } } }

        wait_for { s.connections.size == 2 }
        response = http.get("/api/vhosts/my-connection/channels")
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.as_a.size.should eq 2
        wg.done
      end
    end
  end

  describe "GET /api/channels/:channel" do
    it "should return channel" do
      with_http_server do |http, s|
        with_channel(s) do
          response = http.get("/api/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          name = URI.encode_www_form(body[0]["name"].as_s)
          response = http.get("/api/channels/#{name}")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          expected_keys = ["consumer_details"]
          actual_keys = body.as_h.keys
          expected_keys.each { |k| actual_keys.should contain(k) }
        end
      end
    end

    it "should return message_stats" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("channel_message_stats")
          3.times { q.publish "msg" }
          ch.prefetch(1)

          ch.basic_get(q.name, no_ack: false)
          q.subscribe(no_ack: false) { }
          q.subscribe(no_ack: true) { }

          response = http.get("/api/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          if message_stats = body[0]["message_stats"]?
            message_stats["get"].should eq(1)
            message_stats["deliver"].should eq(1)
            message_stats["deliver_no_ack"].should eq(1)
            message_stats["deliver_get"].should eq(3)
          else
            fail "No channel"
          end
        end
      end
    end

    it "should return get_no_ack count 1" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue
          q.publish "get_no_ack_message"
          q.get(true)
          response = http.get("/api/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)[0]
          if message_stats = body["message_stats"]?
            message_stats["get"].should eq(0)
            message_stats["get_no_ack"].should eq(1)
            message_stats["deliver_get"].should eq(1)
          else
            fail "message_stats is nil"
          end
        end
      end
    end
  end

  describe "PUT /api/channels/:channel" do
    it "should allow to update the prefetch" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          ch.prefetch(5)
          q = ch.queue("")
          q.subscribe { }
          sleep 10.milliseconds

          response = http.get("/api/channels")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          channel = body.as_a.first
          channel["prefetch_count"].should eq 5

          response = http.get("/api/consumers")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.first["prefetch_count"].should eq 5

          body = {"prefetch" => 10}
          url = "/api/channels/#{URI.encode_path(channel["name"].to_s)}"
          response = http.put(url, body: body.to_json)
          response.status_code.should eq 204

          response = http.get(url)
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body["prefetch_count"].should eq 10

          response = http.get("/api/consumers")
          response.status_code.should eq 200
          body = JSON.parse(response.body)
          body.as_a.first["prefetch_count"].should eq 10
        end
      end
    end
  end
end
