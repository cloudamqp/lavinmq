require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: session user" do
    it "checks a taken-over session against the new user" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        server.users.create("bob", "bob")
        server.users.add_permission("bob", "/", /.*/, /.*/, /.*/)
        service = server.vhosts["/"].mqtt_permission_service
        service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
        service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(io)
        end

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "chat/a", payload: "for-alice".to_slice, qos: 0u8)
          pingpong(pub_io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "bob", password: "bob".to_slice)
          # Queued while alice owned the session, so it is delivered.
          msg = read_packet(io)
          msg.should be_a(MQTT::Protocol::Publish)
          msg.as(MQTT::Protocol::Publish).payload.should eq("for-alice".to_slice)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "chat/b", payload: "for-bob".to_slice, qos: 0u8)
            pingpong(pub_io)
          end
          # bob is not a member, so nothing new is accepted into the session.
          read_packet(io).should be_nil
        end
      end
    end

    it "keeps the session user across a restart" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        service = server.vhosts["/"].mqtt_permission_service
        service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
        service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(io)
        end

        restart_server(server)

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "chat/a", payload: "after-restart".to_slice, qos: 0u8)
          pingpong(pub_io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          msg = read_packet(io)
          msg.should be_a(MQTT::Protocol::Publish)
          msg.as(MQTT::Protocol::Publish).payload.should eq("after-restart".to_slice)
        end
      end
    end
  end
end
