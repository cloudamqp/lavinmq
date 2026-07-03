require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  def self.with_http(server, &)
    h = server.http_server
    addr = h.bind_tcp("::1", 0)
    spawn(name: "http listen") { h.listen }
    Fiber.yield
    yield HTTPSpecHelper.new(addr)
  end

  describe "POST /api/queues/vhost/name/peek on MQTT sessions" do
    it "peeks ready messages without consuming them" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))
          disconnect(io)
        end

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "publisher")
          publish(pub_io, topic: "a/b", qos: 1u8, payload: "m1".to_slice)
          publish(pub_io, topic: "a/b", qos: 1u8, payload: "m2".to_slice)
          disconnect(pub_io)
        end

        session = server.vhosts["/"].session("mqtt.client_id")
        wait_for { session.message_count == 2 }

        with_http(server) do |http|
          response = http.post("/api/queues/%2f/mqtt.client_id/peek", body: %({"count": 10}))
          response.status_code.should eq 200
          messages = JSON.parse(response.body).as_a
          messages.map(&.["payload"].as_s).should eq ["m1", "m2"]
          messages.each(&.["state"].as_s.should(eq("ready")))
          session.message_count.should eq 2
        end
      end
    end

    it "peeks unacked messages" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false)
          subscribe(io, topic_filters: mk_topic_filters({"a/b", 1u8}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "publisher")
            publish(pub_io, topic: "a/b", qos: 1u8, payload: "inflight".to_slice)
            disconnect(pub_io)
          end

          # Receive the QoS 1 delivery but do not send PubAck
          read_packet(io).should be_a(MQTT::Protocol::Publish)
          session = server.vhosts["/"].session("mqtt.client_id")
          wait_for { session.@unacked.size == 1 }

          with_http(server) do |http|
            response = http.post("/api/queues/%2f/mqtt.client_id/peek", body: %({"count": 10}))
            response.status_code.should eq 200
            JSON.parse(response.body).as_a.should be_empty

            body = %({"count": 10, "state": "unacked"})
            response = http.post("/api/queues/%2f/mqtt.client_id/peek", body: body)
            response.status_code.should eq 200
            messages = JSON.parse(response.body).as_a
            messages.size.should eq 1
            messages[0]["payload"].as_s.should eq "inflight"
            messages[0]["state"].as_s.should eq "unacked"
            session.@unacked.size.should eq 1
          end
          disconnect(io)
        end
      end
    end
  end
end
