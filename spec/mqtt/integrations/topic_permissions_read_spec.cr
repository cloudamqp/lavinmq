require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: read at delivery" do
    before_each do
      LavinMQ::Config.instance.mqtt_topic_permissions_enabled = true
    end

    after_each do
      LavinMQ::Config.instance.mqtt_topic_permissions_enabled = false
    end

    it "filters a wildcard subscription to only authorized topics" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        # alice has read on chat/alice/#; writer has write on everything
        group = LavinMQ::Auth::PermissionGroup.new(
          "alice-read",
          "mqtt",
          false,
          ["alice"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("chat/alice/#", read: true, write: false),
          ]
        )
        server.permission_groups.put(group)

        writer_group = LavinMQ::Auth::PermissionGroup.new(
          "writer-all",
          "mqtt",
          false,
          ["guest"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("#", read: true, write: true),
          ]
        )
        server.permission_groups.put(writer_group)

        with_client_io(server) do |alice_io|
          # alice subscribes to wildcard "#"
          connect(alice_io, client_id: "alice", username: "alice", password: "alice".to_slice)
          subscribe(alice_io, topic_filters: mk_topic_filters({"#", 0}))

          with_client_io(server) do |writer_io|
            connect(writer_io, client_id: "writer", username: "guest", password: "guest".to_slice)

            publish(writer_io, topic: "chat/alice/room1", payload: "authorized".to_slice, qos: 0u8)
            publish(writer_io, topic: "chat/bob/room1", payload: "unauthorized".to_slice, qos: 0u8)

            # Flush: ensure writer has sent both publishes before we check alice's buffer
            pingpong(writer_io)

            # Alice should receive only the authorized message
            msg = read_packet(alice_io)
            msg.should be_a(MQTT::Protocol::Publish)
            msg = msg.as(MQTT::Protocol::Publish)
            msg.topic.should eq("chat/alice/room1")
            msg.payload.should eq("authorized".to_slice)

            # No further message should arrive
            next_msg = read_packet(alice_io)
            next_msg.should be_nil
          end
        end
      end
    end

    it "filters retained messages on subscribe" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        # alice: read-only on chat/alice/#
        group = LavinMQ::Auth::PermissionGroup.new(
          "alice-read",
          "mqtt",
          false,
          ["alice"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("chat/alice/#", read: true, write: false),
          ]
        )
        server.permission_groups.put(group)

        # guest (the seeder): write on everything so it can seed retained messages
        writer_group = LavinMQ::Auth::PermissionGroup.new(
          "guest-write-all",
          "mqtt",
          false,
          ["guest"],
          [
            LavinMQ::Auth::PermissionGroup::Rule.new("#", read: true, write: true),
          ]
        )
        server.permission_groups.put(writer_group)

        # Seed a retained message on a topic alice is NOT allowed to read
        with_client_io(server) do |seed_io|
          connect(seed_io, client_id: "seeder")
          publish(seed_io, topic: "chat/bob/old", payload: "retained-bob".to_slice, qos: 0u8, retain: true)
          disconnect(seed_io)
        end

        # Also seed a retained message alice IS allowed to read
        with_client_io(server) do |seed_io|
          connect(seed_io, client_id: "seeder2")
          publish(seed_io, topic: "chat/alice/news", payload: "retained-alice".to_slice, qos: 0u8, retain: true)
          disconnect(seed_io)
        end

        with_client_io(server) do |alice_io|
          connect(alice_io, client_id: "alice", username: "alice", password: "alice".to_slice)
          # Subscribe to "#" - retained messages are replayed on subscribe
          subscribe(alice_io, topic_filters: mk_topic_filters({"#", 0}))

          # Alice should receive only the retained message she is allowed to read
          msg = read_packet(alice_io)
          msg.should be_a(MQTT::Protocol::Publish)
          msg = msg.as(MQTT::Protocol::Publish)
          msg.topic.should eq("chat/alice/news")
          msg.payload.should eq("retained-alice".to_slice)

          # chat/bob/old must NOT arrive
          next_msg = read_packet(alice_io)
          next_msg.should be_nil
        end
      end
    end
  end
end
