require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 subscribe" do
    it "disconnects with SubscriptionIdentifiersNotSupported (0xA1) on a Subscription Identifier" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised subscription_identifier_available=0, so any Subscription
          # Identifier is a protocol error -> DISCONNECT 0xA1.
          props = MQTT::Protocol::SubscribeProperties.new
          props.subscription_identifier = 1u32
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 0u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16, props).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::SubscriptionIdentifiersNotSupported)
        end
      end
    end

    it "disconnects with ProtocolError (0x82) on Retain Handling 3 (§3.8.3.1)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The shard cannot encode Retain Handling 3, so send raw bytes: packet
          # id 1, empty properties, filter "a", options 0x30.
          io.write_bytes_raw(Bytes[0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 0x61, 0x30])
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end

    it "disconnects with SharedSubscriptionsNotSupported (0x9E) on a $share/ filter" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised shared_subscription_available=0 (§3.2.2.3.13).
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("$share/group/test/topic", 0u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::SharedSubscriptionsNotSupported)
        end
      end
    end

    it "disconnects on a $share/ filter even when mixed with a normal filter" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # A Shared Subscription anywhere in the packet is a packet-level
          # protocol error -> the whole connection is disconnected, not a
          # per-filter SUBACK reason code.
          tfs = [
            MQTT::Protocol::Subscribe::TopicFilter.new("plain/topic", 0u8),
            MQTT::Protocol::Subscribe::TopicFilter.new("$share/group/test/topic", 0u8),
          ]
          MQTT::Protocol::Subscribe.new(tfs, 1u16).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::SharedSubscriptionsNotSupported)
        end
      end
    end

    it "closes rather than send a SUBACK over the client's Maximum Packet Size [MQTT-3.1.2-24]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          props = MQTT::Protocol::ConnectProperties.new
          props.maximum_packet_size = 30u32
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "sub",
            properties: props).should be_a(MQTT::Protocol::Connack)
          # One reason code per filter, so 30 filters make a 35-byte SUBACK.
          filters = (1..30).map { |i| subtopic("t/#{i}", 0) }
          subscribe(io, topic_filters: filters, packet_id: 1u16, expect_response: false)
          io.should be_closed
        end
      end
    end

    it "grants a QoS 2 subscription as QoS 2" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The SUBACK reports the granted maximum [MQTT-3.8.4-7].
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 2u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16).to_io(io)
          io.flush

          suback = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq([MQTT::Protocol::SubAck::ReasonCode::GrantedQos2])
        end
      end
    end
  end
end
