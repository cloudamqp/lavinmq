require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "duplicate messages" do
    it "dup must not be set if qos is 0 [MQTT-3.3.1-2]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          # Subscribe with qos=0 means downgrade messages to qos=0
          topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("a/b", 0u8)
          subscribe(io, topic_filters: [topic_filter])

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", qos: 0u8)
            publish(publisher_io, topic: "a/b", qos: 1u8)
            disconnect(publisher_io)
          end

          pub1 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub1.qos.should eq(0u8)
          pub1.dup?.should be_false
          pub2 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub2.qos.should eq(0u8)
          pub2.dup?.should be_false

          disconnect(io)
        end
      end
    end

    it "dup is set when a message is being redelivered [MQTT-3.3.1.-1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("a/b", 1u8)
          subscribe(io, topic_filters: [topic_filter])

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", qos: 1u8)
            disconnect(publisher_io)
          end

          pub = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub.dup?.should be_false
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io)
          pub = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub.dup?.should be_true
          disconnect(io)
        end
      end
    end

    it "dup on incoming messages is not propagated to other clients [MQTT-3.3.1-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          # Subscribe with qos=0 means downgrade messages to qos=0
          topic_filter = MQTT::Protocol::Subscribe::TopicFilter.new("a/b", 1u8)
          subscribe(io, topic_filters: [topic_filter])

          with_client_io(server) do |publisher_io|
            connect(publisher_io, client_id: "publisher")
            publish(publisher_io, topic: "a/b", qos: 1u8, dup: true)
            publish(publisher_io, topic: "a/b", qos: 1u8, dup: true)
            disconnect(publisher_io)
          end

          pub1 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub1.dup?.should be_false
          pub2 = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::Publish)
          pub2.dup?.should be_false

          puback(io, pub1.packet_id)
          puback(io, pub2.packet_id)

          disconnect(io)
        end
      end
    end
  end
end
