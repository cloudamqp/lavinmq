require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "publish" do
    it "should return PubAck for QoS=1" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          payload = Bytes[1, 254, 200, 197, 123, 4, 87]
          ack = publish(io, topic: "test", payload: payload, qos: 1u8)
          ack.should be_a(MQTT::Protocol::PubAck)
        end
      end
    end

    it "shouldn't return anything for QoS=0" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)

          payload = Bytes[1, 254, 200, 197, 123, 4, 87]
          ack = publish(io, topic: "test", payload: payload, qos: 0u8)
          ack.should be_nil
        end
      end
    end

    it "sends the whole payload when the session is closed during the send" do
      with_server do |server|
        # Larger than the socket buffers, so the send blocks mid-payload
        payload = Bytes.new(32 * 1024 * 1024) { |i| (i % 251).to_u8 }
        with_client_io(server) do |sub|
          connect(sub, client_id: "slow")
          subscribe(sub, topic_filters: mk_topic_filters({"t", 0u8}))
          with_client_io(server) do |pub|
            connect(pub, client_id: "publisher")
            publish(pub, topic: "t", payload: payload, qos: 0u8)
            disconnect(pub)
          end
          session = server.vhosts["/"].session("mqtt.slow")
          # Published asynchronously, wait for it to be stored in its own segment
          wait_for { session.@msg_store.@segments.last_value.size > payload.size }
          segment = session.@msg_store.@segments.last_value
          # Shifted, and the send is blocked as the subscriber doesn't read
          wait_for { session.message_count.zero? }
          session.close
          segment.closed?.should be_false
          packet = read_packet(sub).as(MQTT::Protocol::Publish)
          packet.payload.should eq payload
          wait_for { segment.closed? }
        end
      end
    end
  end
end
