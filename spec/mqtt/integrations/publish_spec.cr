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

    it "closes the connection on a publish with a topic longer than 255 bytes" do
      with_server do |server|
        with_client_io(server) do |sub|
          connect(sub, client_id: "sub", clean_session: false)
          subscribe(sub, topic_filters: [subtopic("#", 1u8)])

          with_client_io(server) do |io|
            connect(io, client_id: "pub")
            expect_raises(IO::Error) do
              publish(io, topic: "a/#{"x" * 300}", qos: 1u8)
            end
          end

          with_client_io(server) do |io|
            connect(io, client_id: "pub")
            publish(io, topic: "a/b", qos: 1u8).should be_a(MQTT::Protocol::PubAck)
            disconnect(io)
          end

          pub = read_publish(sub)
          pub.topic.should eq "a/b"
          puback(sub, pub.packet_id)
          server.vhosts["/"].session("mqtt.sub").closed?.should be_false
        end
      end
    end

    it "logs a warning when closing a connection over a too long topic" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          Log.capture("lmq.mqtt.client", :warn) do |logs|
            expect_raises(IO::Error) do
              publish(io, topic: "a/#{"x" * 300}", qos: 1u8)
            end
            logs.check("a warning carrying the topic length") do |entry|
              ex = entry.exception
              entry.severity.warn? && ex.is_a?(LavinMQ::MQTT::TopicTooLong) &&
                ex.message == "topic is 302 bytes, max 255"
            end
          end
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
