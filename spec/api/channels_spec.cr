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
        with_channel(s, vhost: "my-connection") do
          with_channel(s, vhost: "my-connection") do
            response = http.get("/api/vhosts/my-connection/channels")
            response.status_code.should eq 200
            body = JSON.parse(response.body)
            body.as_a.size.should eq 2
          end
        end
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

  describe "DELETE /api/channels/:channel" do
    it "should close the channel and tell the client why" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          close_code = 0_u16
          close_text = ""
          ch.on_close do |code, text|
            close_code = code
            close_text = text
          end

          hdrs = ::HTTP::Headers{"X-Reason" => "Misbehaving client"}
          response = http.delete("/api/channels/#{channel_name(http)}", headers: hdrs)
          response.status_code.should eq 204

          wait_for { close_code != 0 }
          close_code.should eq 406
          close_text.should contain "Misbehaving client"
          wait_for { JSON.parse(http.get("/api/channels").body).as_a.empty? }
        end
      end
    end

    it "should requeue the channel's unacked messages" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          q = ch.queue("close_channel_requeue")
          q.publish_confirm "msg"
          q.get(no_ack: false).should_not be_nil
          queue = s.vhosts["/"].queue("close_channel_requeue")
          queue.message_count.should eq 0

          response = http.delete("/api/channels/#{channel_name(http)}")
          response.status_code.should eq 204

          wait_for { queue.message_count == 1 }
        end
      end
    end

    it "should remove the channel without waiting for close-ok" do
      with_http_server do |http, s|
        with_raw_amqp_connection(s) do |io, stream|
          io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(1_u16), IO::ByteFormat::NetworkEndian
          io.flush
          stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)

          name = channel_name(http)
          connection = s.connections.first
          connection.channel_count.should eq 1

          http.delete("/api/channels/#{name}").status_code.should eq 204
          connection.channel_count.should eq 0
          JSON.parse(http.get("/api/channels").body).as_a.empty?.should be_true

          stream.next_frame.should be_a(AMQ::Protocol::Frame::Channel::Close)
        end
      end
    end

    it "should return 404 if the channel does not exist" do
      with_http_server do |http, _|
        response = http.delete("/api/channels/no-such-channel")
        response.status_code.should eq 404
      end
    end

    it "should refuse to close another user's channel" do
      with_http_server do |http, s|
        s.users.create("bob", "pw", [LavinMQ::Tag::Management])
        s.users.add_permission("bob", "/", /.*/, /.*/, /.*/)
        with_channel(s) do
          hdrs = ::HTTP::Headers{"Authorization" => "Basic Ym9iOnB3"} # bob:pw
          response = http.delete("/api/channels/#{channel_name(http)}", headers: hdrs)
          response.status_code.should eq 403
        end
      end
    end
  end
end

# The name of the first open channel, encoded for use in a request path
private def channel_name(http) : String
  body = JSON.parse(http.get("/api/channels").body)
  URI.encode_path(body[0]["name"].as_s)
end
