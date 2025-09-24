require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "message routing" do
    topic = "a/b/c"
    positive_topic_filters = {
      "a/b/c",
      "#",
      "a/#",
      "a/b/#",
      "a/b/+",
      "a/+/+",
      "+/+/+",
      "+/+/c",
      "+/b/c",
      "+/#",
      "+/+/#",
      "a/+/#",
      "a/+/c",
    }
    negative_topic_filters = {
      "c/a/b",
      "c/#",
      "+/a/+",
      "c/+/#",
      "+/+/d",
    }
    positive_topic_filters.each do |topic_filter|
      it "should route #{topic} to #{topic_filter}" do
        with_server do |server|
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic(topic_filter, 1u8)])

            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b/c", qos: 0u8)
            end

            begin
              packet = read_packet(sub).should be_a(MQTT::Protocol::Publish)
              packet.topic.should eq "a/b/c"
            rescue
              fail "timeout; message not routed"
            end
          end
        end
      end
    end

    negative_topic_filters.each do |topic_filter|
      it "should not route #{topic} to #{topic_filter}" do
        with_server do |server|
          with_client_io(server) do |sub|
            connect(sub, client_id: "sub")
            subscribe(sub, topic_filters: [subtopic(topic_filter, 1u8)])

            with_client_io(server) do |pub_io|
              connect(pub_io, client_id: "pub")
              publish(pub_io, topic: "a/b/c", qos: 0u8)
            end

            expect_raises(::IO::TimeoutError) { MQTT::Protocol::Packet.from_io(sub) }
          end
        end
      end
    end
  end
end
