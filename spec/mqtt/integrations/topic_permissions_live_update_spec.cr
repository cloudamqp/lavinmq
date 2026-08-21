require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: live updates" do
    it "stops publishes once write is revoked on a live connection" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        server.vhosts["/"].mqtt_permission_service.put(LavinMQ::MQTT::PermissionGroup.new(
          "g", "/", ["*"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat/#", read: true, write: true)]))

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"chat/#", 0}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub", username: "alice", password: "alice".to_slice)

            publish(pub_io, topic: "chat/1", payload: "one".to_slice, qos: 0u8)
            read_packet(sub_io).should be_a(MQTT::Protocol::Publish)

            server.vhosts["/"].mqtt_permission_service.put(LavinMQ::MQTT::PermissionGroup.new(
              "g", "/", ["*"],
              [LavinMQ::MQTT::PermissionGroup::Rule.new("chat/#", read: true, write: false)]))

            publish(pub_io, topic: "chat/2", payload: "two".to_slice, qos: 0u8)
            ping(pub_io)
            pingpong(pub_io)

            read_packet(sub_io).should be_nil
            pub_io.should_not be_closed
          end
        end
      end
    end

    it "stops deliveries once read is revoked on a live connection" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        server.vhosts["/"].mqtt_permission_service.put(LavinMQ::MQTT::PermissionGroup.new(
          "g", "/", ["*"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat/#", read: true, write: true)]))

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"chat/#", 0}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub", username: "alice", password: "alice".to_slice)

            publish(pub_io, topic: "chat/1", payload: "one".to_slice, qos: 0u8)
            read_packet(sub_io).should be_a(MQTT::Protocol::Publish)

            server.vhosts["/"].mqtt_permission_service.put(LavinMQ::MQTT::PermissionGroup.new(
              "g", "/", ["*"],
              [LavinMQ::MQTT::PermissionGroup::Rule.new("chat/#", read: false, write: true)]))

            publish(pub_io, topic: "chat/2", payload: "two".to_slice, qos: 0u8)
            ping(pub_io)
            pingpong(pub_io)

            read_packet(sub_io).should be_nil
          end
        end
      end
    end
  end
end
