require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "publish and subscribe flow" do
    topic = "a/b/c"
    {"a/b/c", "a/#", "a/+/c", "a/b/#", "a/b/+", "#"}.each do |topic_filter|
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
  end

  describe "session handling" do
    it "messages are delivered to client that connects to a existing session" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false)
          subscribe(io, topic_filters: [subtopic("a/b/c", 1u8)])
          disconnect(io)
        end

        with_client_io(server) do |io|
          connect(io, clean_session: true, client_id: "pub")
          publish(io, topic: "a/b/c", qos: 0u8)
        end

        with_client_io(server) do |io|
          connect(io, clean_session: false)
          packet = read_packet(io).should be_a(MQTT::Protocol::Publish)
          packet.topic.should eq "a/b/c"
        rescue
          fail "timeout; message not routed"
        end
      end
    end
  end
end
