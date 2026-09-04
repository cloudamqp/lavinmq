require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT topic permissions: publish" do
    it "does not restrict publish or subscribe when no permission groups exist" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          subscribe(sub_io, topic_filters: mk_topic_filters({"anything/#", 0}))

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "alice", username: "alice", password: "alice".to_slice)
            publish(pub_io, topic: "anything/goes", payload: "hi".to_slice, qos: 0u8)

            msg = read_packet(sub_io)
            msg.should be_a(MQTT::Protocol::Publish)
            msg.as(MQTT::Protocol::Publish).topic.should eq("anything/goes")
          end
        end
      end
    end

    it "delivers authorized publishes and drops unauthorized ones, keeping connection open" do
      with_server do |server|
        # Create user "alice" with vhost access
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        # Grant alice write on chat/{client_id}/# and read on chat/#. Both
        # connections authenticate as alice, so the subscriber gets the broad
        # read rule and the publisher may only write under its own client id.
        group = LavinMQ::MQTT::PermissionGroup.new(
          "alice-chat", "/",
          ["alice"],
          [
            LavinMQ::MQTT::PermissionGroup::Rule.new("chat--client-id---", "chat/{client_id}/#", read: true, write: true),
            LavinMQ::MQTT::PermissionGroup::Rule.new("chat--", "chat/#", read: true, write: false),
          ]
        )
        server.vhosts["/"].mqtt_permission_service.put(group)

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          topic_filters = mk_topic_filters({"chat/#", 0})
          subscribe(sub_io, topic_filters: topic_filters)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "alice", username: "alice", password: "alice".to_slice)

            # Publish to authorized topic: chat/alice/room1 matches chat/{client_id}/#
            # for the connection whose client_id is "alice"
            publish(pub_io, topic: "chat/alice/room1", payload: "hello".to_slice, qos: 0u8)

            # Publish to unauthorized topic: chat/other/secret does not match
            publish(pub_io, topic: "chat/other/secret", payload: "secret".to_slice, qos: 0u8)

            # Send a ping to flush any buffered events (ensures the second publish
            # would arrive at sub if it was going to)
            ping(pub_io)
            pingpong(pub_io)

            # Subscriber should receive only the authorized message
            msg = read_packet(sub_io)
            msg.should be_a(MQTT::Protocol::Publish)
            msg = msg.as(MQTT::Protocol::Publish)
            msg.topic.should eq("chat/alice/room1")
            msg.payload.should eq("hello".to_slice)

            # No second message should arrive
            next_msg = read_packet(sub_io)
            next_msg.should be_nil

            # Publisher connection must still be open
            pub_io.should_not be_closed
          end
        end
      end
    end

    it "acks a denied qos>0 publish but does not deliver it, keeping connection open" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)

        group = LavinMQ::MQTT::PermissionGroup.new(
          "alice-chat", "/",
          ["alice", "sub"],
          [
            LavinMQ::MQTT::PermissionGroup::Rule.new("chat--client-id---", "chat/{client_id}/#", read: true, write: true),
            LavinMQ::MQTT::PermissionGroup::Rule.new("chat--", "chat/#", read: true, write: false),
          ]
        )
        server.vhosts["/"].mqtt_permission_service.put(group)

        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub", username: "alice", password: "alice".to_slice)
          topic_filters = mk_topic_filters({"chat/#", 0})
          subscribe(sub_io, topic_filters: topic_filters)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "alice", username: "alice", password: "alice".to_slice)

            # Publish to a denied topic with qos=1 and a packet_id. The publish
            # is dropped but a PubAck must still be returned so client flow
            # continues.
            packet_id = next_packet_id
            ack = publish(pub_io,
              topic: "chat/other/secret",
              payload: "secret".to_slice,
              qos: 1u8,
              packet_id: packet_id
            )
            ack.should be_a(MQTT::Protocol::PubAck)
            ack.as(MQTT::Protocol::PubAck).packet_id.should eq(packet_id)

            # The denied message must not be delivered to the subscriber.
            next_msg = read_packet(sub_io)
            next_msg.should be_nil

            # Publisher connection must still be open.
            pub_io.should_not be_closed
          end
        end
      end
    end
  end
end
