require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  def self.serve_http(server)
    h = server.http_server
    addr = h.bind_tcp("::1", 0)
    spawn(name: "http listen") { h.listen }
    Fiber.yield
    HTTPSpecHelper.new(addr)
  end

  describe "MQTT topic permissions: HTTP update" do
    it "applies an HTTP PUT that replaces write with read on a live connection" do
      with_server do |server|
        http = MqttSpecs.serve_http(server)

        create = {members: ["*"],
                  rules:   [{pattern: "chat/#", read: true, write: true}]}.to_json
        http.put("/api/permission-groups/g", body: create).status_code.should eq 201

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "guest", password: "guest".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"chat/#", 0}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub", username: "guest", password: "guest".to_slice)

            publish(pub_io, topic: "chat/1", payload: "one".to_slice, qos: 0u8)
            read_packet(sub_io).should be_a(MQTT::Protocol::Publish)

            # Same group name, rule replaced: write revoked, read kept.
            update = {members: ["*"],
                      rules:   [{pattern: "chat/#", read: true, write: false}]}.to_json
            http.put("/api/permission-groups/g", body: update).status_code.should eq 204

            publish(pub_io, topic: "chat/2", payload: "two".to_slice, qos: 0u8)
            ping(pub_io)
            pingpong(pub_io)

            read_packet(sub_io).should be_nil
          end
        end
      end
    end

    it "applies a read revocation to a persistent session that was offline when it changed" do
      with_server do |server|
        http = MqttSpecs.serve_http(server)

        create = {members: ["*"],
                  rules:   [{pattern: "chat/#", read: true, write: true}]}.to_json
        http.put("/api/permission-groups/g", body: create).status_code.should eq 201

        # Durable session subscribes at QoS 1, then goes away.
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "durable", clean_session: false,
            username: "guest", password: "guest".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(sub_io)
        end

        # Read revoked while the session has no attached client. Write kept so the
        # publisher can still send.
        update = {members: ["*"],
                  rules:   [{pattern: "chat/#", read: false, write: true}]}.to_json
        http.put("/api/permission-groups/g", body: update).status_code.should eq 204

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub", username: "guest", password: "guest".to_slice)
          publish(pub_io, topic: "chat/offline", payload: "nope".to_slice, qos: 1u8,
            packet_id: next_packet_id)
          pingpong(pub_io)
        end

        # The durable session must not hand over a message it is no longer allowed to read.
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "durable", clean_session: false,
            username: "guest", password: "guest".to_slice)
          read_packet(sub_io).should be_nil
        end
      end
    end

    it "still delivers a message accepted into a session before its read access was later revoked" do
      with_server do |server|
        http = MqttSpecs.serve_http(server)

        create = {members: ["*"],
                  rules:   [{pattern: "chat/#", read: true, write: true}]}.to_json
        http.put("/api/permission-groups/g", body: create).status_code.should eq 201

        # Durable session subscribes at QoS 1, then goes away.
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "durable", clean_session: false,
            username: "guest", password: "guest".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(sub_io)
        end

        # Message is accepted into the session while read is still granted.
        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub", username: "guest", password: "guest".to_slice)
          publish(pub_io, topic: "chat/kept", payload: "kept".to_slice, qos: 1u8,
            packet_id: next_packet_id)
          pingpong(pub_io)
        end

        # Read revoked only now, after the message was already accepted.
        update = {members: ["*"],
                  rules:   [{pattern: "chat/#", read: false, write: true}]}.to_json
        http.put("/api/permission-groups/g", body: update).status_code.should eq 204

        # Authorization was settled at accept time: a message let in under a grant
        # that has since been revoked is still handed over. Only messages published
        # after the revocation are refused.
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "durable", clean_session: false,
            username: "guest", password: "guest".to_slice)
          packet = read_packet(sub_io)
          packet.should be_a(MQTT::Protocol::Publish)
          packet.as(MQTT::Protocol::Publish).topic.should eq "chat/kept"
        end
      end
    end
  end
end
