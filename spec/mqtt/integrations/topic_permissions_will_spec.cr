require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # A will is published by the server long after the client that set it is gone,
  # so it is the one publish nobody can retract. It is checked against the same
  # write rules as a live publish, including {client_id}.
  describe "MQTT topic permissions: will" do
    it "publishes a will on a topic the client may write" do
      with_server do |server|
        grant_own_writes(server)

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"#", 0}))

          with_client_io(server) do |will_io|
            will = MQTT::Protocol::Will.new(
              topic: "chat/willy/last", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(will_io, client_id: "willy", username: "alice", password: "alice".to_slice,
              will: will, keepalive: 1u16)
          end

          pub = read_packet(sub_io).should be_a(MQTT::Protocol::Publish)
          pub.topic.should eq("chat/willy/last")
          pub.payload.should eq("dead".to_slice)
        end
      end
    end

    it "drops a will on a topic the client may not write" do
      with_server do |server|
        grant_own_writes(server)

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"#", 0}))

          with_client_io(server) do |will_io|
            # "chat/other/last" is another client's namespace: allowed to read,
            # never to write.
            will = MQTT::Protocol::Will.new(
              topic: "chat/other/last", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(will_io, client_id: "willy", username: "alice", password: "alice".to_slice,
              will: will, keepalive: 1u16)
          end

          # A marker on a topic alice may write. The will was published first if
          # at all, so receiving the marker first proves it was dropped.
          publish(sub_io, topic: "chat/sub/marker", payload: "alive".to_slice)

          pub = read_packet(sub_io).should be_a(MQTT::Protocol::Publish)
          pub.topic.should eq("chat/sub/marker")
          pub.payload.should eq("alive".to_slice)
        end
      end
    end

    it "does not retain a will the client may not write" do
      with_server do |server|
        grant_own_writes(server)

        with_client_io(server) do |will_io|
          will = MQTT::Protocol::Will.new(
            topic: "chat/other/last", payload: "dead".to_slice, qos: 0u8, retain: true)
          connect(will_io, client_id: "willy", username: "alice", password: "alice".to_slice,
            will: will, keepalive: 1u16)
        end

        # A denied retained will must leave nothing behind for the next
        # subscriber either.
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"#", 0}))
          read_packet(sub_io).should be_nil
        end
      end
    end
  end
end

# alice may write only under her own client id, and read everything.
private def grant_own_writes(server)
  server.users.create("alice", "alice")
  server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
  group = LavinMQ::MQTT::PermissionGroup.new(
    "alice-will", "/",
    ["alice"],
    [
      LavinMQ::MQTT::PermissionGroup::Rule.new("own", "chat/{client_id}/#", read: true, write: true),
      LavinMQ::MQTT::PermissionGroup::Rule.new("all", "#", read: true, write: false),
    ]
  )
  server.vhosts["/"].mqtt_permission_service.delete("default")
  server.vhosts["/"].mqtt_permission_service.put(group)
end
