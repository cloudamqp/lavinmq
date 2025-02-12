require "../spec_helper.cr"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "retained messages" do
    it "retained messages are received on subscribe" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          publish(io, topic: "a/b", qos: 0u8, retain: true)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("a/b")])
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.topic.should eq("a/b")
          pub.retain?.should eq(true)
          disconnect(io)
        end
      end
    end

    it "retain flag is 0 if there is an established subscription [MQTT-3.3.1-9]" do
      with_server do |server|
        with_client_io(server) do |sub_io|
          connect(sub_io, client_id: "sub")
          topic_filters = mk_topic_filters({"test", 0})
          subscribe(sub_io, topic_filters: topic_filters)

          with_client_io(server) do |pub_io|
            connect(pub_io, client_id: "pub")
            publish(pub_io, topic: "test", qos: 0u8, retain: true)
          end

          msg = read_packet(sub_io).as(MQTT::Protocol::Publish)
          msg.retain?.should eq(false)
        end
      end
    end

    it "retained messages are redelivered for subscriptions with qos1" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "publisher")
          publish(io, topic: "a/b", qos: 0u8, retain: true)
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          subscribe(io, topic_filters: [subtopic("a/b", 1u8)])
          # Dont ack
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.qos.should eq(1u8)
          pub.topic.should eq("a/b")
          pub.retain?.should eq(true)
          pub.dup?.should eq(false)
        end

        with_client_io(server) do |io|
          connect(io, client_id: "subscriber")
          pub = read_packet(io).as(MQTT::Protocol::Publish)
          pub.qos.should eq(1u8)
          pub.topic.should eq("a/b")
          pub.retain?.should eq(true)
          pub.dup?.should eq(true)
          puback(io, pub.packet_id)
        end
      end
    end
  end
end
