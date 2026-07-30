require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: subscribe" do
    it "accepts a subscribe to a filter it cannot read and delivers nothing on it" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        # alice may read only her own subtree.
        server.permission_service.put(LavinMQ::Auth::PermissionGroup.new(
          "alice-read", "mqtt", ["alice"],
          [LavinMQ::Auth::PermissionGroup::Rule.new("chat/alice/#", read: true, write: false)]))
        # The publisher may write anywhere under chat/, so a message to chat/bob
        # really is published and routed. Only alice's read rule can stop it.
        server.permission_service.put(LavinMQ::Auth::PermissionGroup.new(
          "pub-write", "mqtt", ["pub"],
          [LavinMQ::Auth::PermissionGroup::Rule.new("chat/#", read: false, write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "alice", username: "alice", password: "alice".to_slice)

          topic_filters = mk_topic_filters({"chat/alice/room1", 1}, {"chat/bob/#", 1})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          suback.return_codes.size.should eq(2)
          # Both accepted: filtering happens when a message is accepted into the session.
          suback.return_codes[0].should eq(MQTT::Protocol::SubAck::ReturnCode::QoS1)
          suback.return_codes[1].should eq(MQTT::Protocol::SubAck::ReturnCode::QoS1)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub", username: "alice", password: "alice".to_slice)
            # Readable by alice, so it must arrive.
            publish(pub_io, topic: "chat/alice/room1", payload: "yes".to_slice, qos: 0u8)
            # Not readable by alice, though she holds a subscription matching it.
            publish(pub_io, topic: "chat/bob/secret", payload: "no".to_slice, qos: 0u8)
            ping(pub_io)
            pingpong(pub_io)
          end

          msg = read_packet(io)
          msg.should be_a(MQTT::Protocol::Publish)
          msg.as(MQTT::Protocol::Publish).topic.should eq("chat/alice/room1")
          read_packet(io).should be_nil
        end
      end
    end
  end
end
