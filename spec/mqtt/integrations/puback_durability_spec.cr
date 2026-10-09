require "../spec_helper"

# Lets a spec hold the publish confirm loop's drain, so what is acked before
# the data is durable can be observed
class LavinMQ::Persister
  class_property drain_gate : ::Channel(Nil)? = nil

  private def drain : Nil
    @@drain_gate.try &.receive?
    previous_def
  end
end

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  def self.with_drain_held(&)
    gate = ::Channel(Nil).new
    LavinMQ::Persister.drain_gate = gate
    begin
      yield gate
    ensure
      LavinMQ::Persister.drain_gate = nil
      gate.close
    end
  end

  def self.release_drain(gate) : Nil
    LavinMQ::Persister.drain_gate = nil
    gate.close
  end

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
end
