require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "client will" do
    it "will is not delivered on graceful disconnect [MQTT-3.14.4-3]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"#", 0})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
            disconnect(io2)
          end

          # If the will has been published it should be received before this
          publish(io, topic: "a/b", payload: "alive".to_slice)

          pub = read_packet(io)

          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq("alive".to_slice)
          pub.topic.should eq("a/b")

          disconnect(io)
        end
      end
    end

    it "will is delivered on ungraceful disconnect" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"will/t", 0})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
          end

          pub = read_packet(io)

          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq("dead".to_slice)
          pub.topic.should eq("will/t")

          disconnect(io)
        end
      end
    end

    it "will can be retained [MQTT-3.1.2-17]" do
      with_server do |server|
        with_client_io(server) do |io2|
          will = MQTT::Protocol::Will.new(
            topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: true)
          connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
        end

        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"will/t", 0})
          subscribe(io, topic_filters: topic_filters)

          pub = read_packet(io)

          pub.should be_a(MQTT::Protocol::Publish)
          pub = pub.as(MQTT::Protocol::Publish)
          pub.payload.should eq("dead".to_slice)
          pub.topic.should eq("will/t")
          pub.retain?.should eq(true)

          disconnect(io)
        end
      end
    end

    it "will won't be published if missing permission" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          topic_filters = mk_topic_filters({"topic-without-permission/t", 0})
          subscribe(io, topic_filters: topic_filters)

          with_client_io(server) do |io2|
            will = MQTT::Protocol::Will.new(
              topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
            connect(io2, client_id: "will_client", will: will, keepalive: 1u16)
          end

          # Send a ping to ensure we can read at least one packet, so we're not stuck
          # waiting here (since this spec verifies that nothing is sent)
          ping(io)

          pkt = read_packet(io)
          pkt.should be_a(MQTT::Protocol::PingResp)

          disconnect(io)
        end
      end
    end

    it "will qos can't be set of will flag is unset [MQTT-3.1.2-13]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO.new(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0001_0000u8
          io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end

    it "will qos must not be 3 [MQTT-3.1.2-14]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          will = MQTT::Protocol::Will.new(
            topic: "will/t", payload: "dead".to_slice, qos: 0u8, retain: false)
          connect(MQTT::Protocol::IO.new(temp_io), will: will, client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0001_1000u8
          io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end

    it "will retain can't be set of will flag is unset [MQTT-3.1.2-15]" do
      with_server do |server|
        with_client_io(server) do |io|
          temp_io = IO::Memory.new
          connect(MQTT::Protocol::IO.new(temp_io), client_id: "will_client", keepalive: 1u16, expect_response: false)
          temp_io.rewind
          connect_pkt = temp_io.to_slice
          connect_pkt[9] |= 0b0010_0000u8
          io.write connect_pkt

          expect_raises(IO::Error) do
            read_packet(io)
          end
        end
      end
    end
  end
end
