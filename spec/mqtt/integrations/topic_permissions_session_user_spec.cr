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
        service.delete("default")
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
          publish(pub_io, topic: "chat/a", payload: "for-alice".to_slice, qos: 1u8)
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
            publish(pub_io, topic: "chat/b", payload: "for-bob".to_slice, qos: 1u8)
            pingpong(pub_io)
          end
          # bob is not a member, so nothing new is accepted into the session.
          io.should be_silent
        end
      end
    end

    it "keeps the session user across a restart" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        service = server.vhosts["/"].mqtt_permission_service
        service.delete("default")
        service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
        service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(io)
        end

        metadata_file = File.join(server.vhosts["/"].data_dir, Digest::SHA1.hexdigest("mqtt.dev"), ".metadata")
        metadata = JSON.parse(File.read(metadata_file))
        metadata["client_id"].as_s.should eq "dev"
        metadata["username"].as_s.should eq "alice"

        restart_server(server)

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "chat/a", payload: "after-restart".to_slice, qos: 1u8)
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

    it "keeps the user that took over a session across a restart" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        server.users.create("bob", "bob")
        server.users.add_permission("bob", "/", /.*/, /.*/, /.*/)
        service = server.vhosts["/"].mqtt_permission_service
        service.delete("default")
        service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
        service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}))
          disconnect(io)
        end
        # Without a SUBSCRIBE of its own
        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "bob", password: "bob".to_slice)
          pingpong(io)
          disconnect(io)
        end

        restart_server(server)

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "chat/a", payload: "after-restart".to_slice, qos: 1u8, packet_id: 1u16)
          pingpong(pub_io)
        end
        # bob is not a member, so the session took nothing in
        server.vhosts["/"].session("mqtt.dev").message_count.should eq 0
      end
    end

    it "keeps the user of a session bound outside a SUBSCRIBE across a restart" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        service = server.vhosts["/"].mqtt_permission_service
        service.delete("default")
        service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
        service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
          pingpong(io)
          disconnect(io)
        end
        # As the HTTP API or a definitions import would
        server.vhosts["/"].bind_queue("mqtt.dev", LavinMQ::MQTT::EXCHANGE, "chat/#", LavinMQ::MQTT.qos_arguments(1u8))

        restart_server(server)

        with_client_io(server) do |pub_io|
          connect(pub_io, client_id: "pub")
          publish(pub_io, topic: "chat/a", payload: "after-restart".to_slice, qos: 1u8, packet_id: 1u16)
          pingpong(pub_io)
        end
        server.vhosts["/"].session("mqtt.dev").message_count.should eq 1
      end
    end

    it "writes no metadata file for a persistent session that never subscribed" do
      with_server do |server|
        metadata_file = File.join(server.vhosts["/"].data_dir, Digest::SHA1.hexdigest("mqtt.dev"), ".metadata")
        with_client_io(server) do |io|
          connect(io, client_id: "dev", clean_session: false)
          pingpong(io)
          File.exists?(metadata_file).should be_false
          subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}))
          JSON.parse(File.read(metadata_file))["username"].as_s.should eq "guest"
        end
      end
    end

    # An empty file is a parse error, a JSON array is valid JSON that is not
    # an object; both must degrade to an unknown user, never fail the load.
    ["", "[]"].each do |content|
      it "loads a session with a metadata file holding #{content.inspect} and applies wildcard rules only" do
        with_server do |server|
          server.users.create("alice", "alice")
          server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
          service = server.vhosts["/"].mqtt_permission_service
          service.delete("default")
          service.put(LavinMQ::MQTT::PermissionGroup.new("alice-read", "/", ["alice"],
            [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", read: true)]))
          service.put(LavinMQ::MQTT::PermissionGroup.new("public", "/", ["*"],
            [LavinMQ::MQTT::PermissionGroup::Rule.new("public", "public/#", read: true, write: true)]))
          service.put(LavinMQ::MQTT::PermissionGroup.new("guest-write", "/", ["guest"],
            [LavinMQ::MQTT::PermissionGroup::Rule.new("chat", "chat/#", write: true)]))

          with_client_io(server) do |io|
            connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
            subscribe(io, topic_filters: mk_topic_filters({"chat/#", 1}, {"public/#", 1}))
            disconnect(io)
          end

          metadata_file = File.join(server.vhosts["/"].data_dir, Digest::SHA1.hexdigest("mqtt.dev"), ".metadata")
          File.write(metadata_file, content)
          restart_server(server)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "chat/a", payload: "member-only".to_slice, qos: 1u8)
            publish(pub_io, topic: "public/a", payload: "for-all".to_slice, qos: 1u8)
            pingpong(pub_io)
          end

          with_client_io(server) do |io|
            connect(io, client_id: "dev", clean_session: false, username: "alice", password: "alice".to_slice)
            msg = read_packet(io)
            msg.should be_a(MQTT::Protocol::Publish)
            msg.as(MQTT::Protocol::Publish).payload.should eq("for-all".to_slice)
            io.should be_silent
          end
        end
      end
    end
  end
end
