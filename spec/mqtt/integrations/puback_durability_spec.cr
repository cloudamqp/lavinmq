require "../spec_helper"

# Reads from the socket, fails every write
private class WriteFailingIO < IO
  def initialize(@io : IO)
  end

  def read(slice : Bytes)
    @io.read(slice)
  end

  def write(slice : Bytes) : Nil
    raise IO::Error.new("write failed")
  end

  def close
    @io.close
  end

  def closed?
    @io.closed?
  end
end

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "QoS 1 PUBACK durability" do
    it "sends the PUBACK once the publish is durable, without blocking the read loop" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8, expect_response: false)
            ping(io)
            read_packet(io).should be_a(MQTT::Protocol::PingResp)
            read_packet(io).should be_nil # no PUBACK while the drain is held
            release_drain(gate)
            read_packet(io).should be_a(MQTT::Protocol::PubAck)
          end
        end
      end
    end

    it "sends PUBACKs in publish order, also for publishes denied by topic permissions" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        group = LavinMQ::MQTT::PermissionGroup.new(
          "allowed", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("allowed--", "allowed/#", read: true, write: true)]
        )
        server.vhosts["/"].mqtt_permission_service.delete("default")
        server.vhosts["/"].mqtt_permission_service.put(group)

        with_client_io(server) do |io|
          connect(io, client_id: "alice", username: "alice", password: "alice".to_slice)
          with_drain_held do |gate|
            publish(io, topic: "allowed/a", payload: "a".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "denied/b", payload: "b".to_slice, qos: 1u8, packet_id: 2u16, expect_response: false)
            publish(io, topic: "allowed/c", payload: "c".to_slice, qos: 1u8, packet_id: 3u16, expect_response: false)
            pingpong(io)
            release_drain(gate)
            packet_ids = Array.new(3) { read_packet(io).as(MQTT::Protocol::PubAck).packet_id }
            packet_ids.should eq [1u16, 2u16, 3u16]
          end
        end
      end
    end

    it "closes the socket when an acknowledgement cannot be written" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "pub")
          session = server.vhosts["/"].session("mqtt.pub")
          wait_for { session.client }
          client = session.client.not_nil!
          pointerof(client.@io).value = MQTT::Protocol::IO.new(WriteFailingIO.new(client.@io.io))
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
          # Nothing more may follow a packet that may be half written
          io.should be_closed
        end
      end
    end

    it "syncs the session segment a QoS 1 publish is written to" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 1}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "a/b", payload: "a".to_slice, qos: 1u8)
            sync = server.persister.last_sync.not_nil!
            sync.paths.any?(&.ends_with?("msgs.0000000001")).should be_true
          end
        end
      end
    end

    it "syncs the retain index and message file of a retained QoS 1 publish" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "retained/a", payload: "a".to_slice, qos: 1u8, retain: true)
          store_dir = File.join(server.vhosts["/"].data_dir, "mqtt_retained_store")
          server.persister.last_sync.not_nil!.paths.should contain File.join(store_dir, "index")
          Dir.children(store_dir).none?(&.ends_with?(".tmp")).should be_true
        end
      end
    end

    it "syncs a QoS 1 retained message's file and directory through the persister" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "r/t", payload: "retained".to_slice, qos: 1u8, retain: true)
          dir = File.join(server.vhosts["/"].data_dir, "mqtt_retained_store")
          paths = server.persister.last_sync.not_nil!.paths
          paths.should contain dir
          paths.any? { |p| File.dirname(p) == dir && p.ends_with?(LavinMQ::MQTT::RetainStore::MESSAGE_FILE_SUFFIX) }.should be_true
        end
      end
    end
  end

  describe "QoS 2 PUBREC durability" do
    it "sends the PUBREC once the publish is durable, without blocking the read loop" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16, expect_response: false)
            ping(io)
            read_packet(io).should be_a(MQTT::Protocol::PingResp)
            read_packet(io).should be_nil # no PUBREC while the drain is held
            release_drain(gate)
            read_packet(io).as(MQTT::Protocol::PubRec).packet_id.should eq 1u16
          end
        end
      end
    end

    it "sends the PUBCOMP once the release is durable [MQTT-4.3.3-2]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          publish(io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16)
          with_drain_held do |gate|
            pubrel(io, 1u16)
            ping(io)
            read_packet(io).should be_a(MQTT::Protocol::PingResp)
            read_packet(io).should be_nil # no PUBCOMP while the drain is held
            release_drain(gate)
            read_packet(io).as(MQTT::Protocol::PubComp).packet_id.should eq 1u16
          end
        end
      end
    end

    it "sends the PUBREL to a subscriber once the PUBREC is durable" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          pub = deliver_qos2(server, sub_io)
          with_drain_held do |gate|
            pubrec(sub_io, pub.packet_id.as(UInt16))
            ping(sub_io)
            read_packet(sub_io).should be_a(MQTT::Protocol::PingResp)
            read_packet(sub_io).should be_nil
            release_drain(gate)
            read_packet(sub_io).as(MQTT::Protocol::PubRel).packet_id.should eq pub.packet_id
          end
        end
      end
    end

    it "answers a PUBLISH re-sent before the first copy is durable only once it is" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          with_drain_held do |gate|
            publish(io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16, dup: true, expect_response: false)
            pingpong(io)
            read_packet(io).should be_nil
            release_drain(gate)
            2.times { read_packet(io).as(MQTT::Protocol::PubRec).packet_id.should eq 1u16 }
          end
        end
      end
    end

    it "sends PUBACKs and PUBRECs in receive order, also for denied topics" do
      with_server do |server|
        server.users.create("alice", "alice")
        server.users.add_permission("alice", "/", /.*/, /.*/, /.*/)
        group = LavinMQ::MQTT::PermissionGroup.new(
          "allowed", "/", ["alice"],
          [LavinMQ::MQTT::PermissionGroup::Rule.new("allowed--", "allowed/#", read: true, write: true)]
        )
        server.vhosts["/"].mqtt_permission_service.delete("default")
        server.vhosts["/"].mqtt_permission_service.put(group)

        with_client_io(server) do |io|
          connect(io, client_id: "alice", username: "alice", password: "alice".to_slice)
          with_drain_held do |gate|
            publish(io, topic: "allowed/a", payload: "a".to_slice, qos: 1u8, packet_id: 1u16, expect_response: false)
            publish(io, topic: "allowed/b", payload: "b".to_slice, qos: 2u8, packet_id: 2u16, expect_response: false)
            publish(io, topic: "denied/c", payload: "c".to_slice, qos: 2u8, packet_id: 3u16, expect_response: false)
            publish(io, topic: "allowed/d", payload: "d".to_slice, qos: 1u8, packet_id: 4u16, expect_response: false)
            pingpong(io)
            release_drain(gate)
            acks = Array.new(4) do
              case packet = read_packet(io)
              when MQTT::Protocol::PubAck then {:puback, packet.packet_id}
              when MQTT::Protocol::PubRec then {:pubrec, packet.packet_id}
              else                             fail "unexpected #{packet.inspect}"
              end
            end
            acks.should eq [{:puback, 1u16}, {:pubrec, 2u16}, {:pubrec, 3u16}, {:puback, 4u16}]
          end
        end
      end
    end

    # The exchange marks QoS 2 publishes for syncing, but only a queued
    # acknowledgement makes the persister sync them
    it "syncs the session segment a QoS 2 publish is written to" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          subscribe(sub_io, topic_filters: mk_topic_filters({"a/b", 2}))
          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            # A durable publisher's PUBREC waits a second drain, for the packet
            # id log, so the segment is in the first one
            with_drain_held do |gate|
              publish(pub_io, topic: "a/b", payload: "a".to_slice, qos: 2u8, packet_id: 1u16, expect_response: false)
              step_drain(gate)
              wait_for { server.persister.last_sync.try &.paths.any?(&.ends_with?("msgs.0000000001")) }
              release_drain(gate)
              read_packet(pub_io).should be_a(MQTT::Protocol::PubRec)
            end
          end
        end
      end
    end
  end
end
