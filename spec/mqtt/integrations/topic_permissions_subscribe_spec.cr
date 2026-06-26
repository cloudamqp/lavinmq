require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: subscribe" do
    before_each do
      LavinMQ::Config.instance.mqtt_topic_permissions_enabled = true
    end

    after_each do
      LavinMQ::Config.instance.mqtt_topic_permissions_enabled = false
    end

    it "returns SUBACK failure for zero-overlap filter and granted QoS for overlapping one" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        # Grant alice read on chat/alice/#
        group = LavinMQ::Auth::PermissionGroup.new(
          "alice-chat",
          "mqtt",
          false,
          ["alice"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("chat/alice/#", read: true, write: false),
          ]
        )
        server.permission_groups.put(group)

        with_client_io(server) do |io|
          connect(io, client_id: "alice", username: "alice", password: "alice".to_slice)

          topic_filters = mk_topic_filters({"chat/alice/room1", 1}, {"chat/bob/#", 1})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          suback.return_codes.size.should eq(2)
          suback.return_codes[0].should eq(MQTT::Protocol::SubAck::ReturnCode::QoS1)
          suback.return_codes[1].should eq(MQTT::Protocol::SubAck::ReturnCode::Failure)
        end
      end
    end

    it "allows a broad # subscription that overlaps an allowed prefix" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        group = LavinMQ::Auth::PermissionGroup.new(
          "alice-chat",
          "mqtt",
          false,
          ["alice"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("chat/alice/#", read: true, write: false),
          ]
        )
        server.permission_groups.put(group)

        with_client_io(server) do |io|
          connect(io, client_id: "alice", username: "alice", password: "alice".to_slice)

          topic_filters = mk_topic_filters({"#", 1})
          suback = subscribe(io, topic_filters: topic_filters)
          suback.should be_a(MQTT::Protocol::SubAck)
          suback = suback.as(MQTT::Protocol::SubAck)
          suback.return_codes.size.should eq(1)
          suback.return_codes[0].should eq(MQTT::Protocol::SubAck::ReturnCode::QoS1)
        end
      end
    end
  end
end
