require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "publish" do
    it "tracks QoS 1 session writes and drains them before PUBACK" do
      with_server do |server|
        with_client_io(server) do |subscriber|
          connect(subscriber, client_id: "durable-subscriber", clean_session: false)
          subscribe(subscriber, topic_filters: [subtopic("durable-topic")])
          broker = server.mqtt_server.broker("/")
          session = broker.sessions["durable-subscriber"]
          file = session.@msg_store.@wfile
          broker.@exchange.publish(publish_packet(topic: "durable-topic", payload: "first".to_slice, qos: 0u8))
          file.@needs_msync.get.should be_false
          broker.@exchange.publish(publish_packet(topic: "durable-topic", payload: "second".to_slice, qos: 1u8))
          file.@needs_msync.get.should be_true
          with_client_io(server) do |publisher|
            connect(publisher, client_id: "publisher")
            publish(publisher, topic: "durable-topic", payload: "third".to_slice, qos: 1u8).should be_a(MQTT::Protocol::PubAck)
            file.@needs_msync.get.should be_false
          end
        end
      end
    end

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
  end
end
