require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 subscribe" do
    it "disconnects with SubscriptionIdentifiersNotSupported (0xA1) on a Subscription Identifier" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
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

    it "disconnects with SharedSubscriptionsNotSupported (0x9E) on a $share/ filter" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised shared_subscription_available=0 [MQTT-3.2.2.3.13].
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
          io = MQTT::Protocol::IO::V5.new(socket)
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

    it "clamps the granted QoS in SUBACK to the server maximum" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We only deliver up to QoS 1, so a QoS 2 request is granted QoS 1
          # [MQTT-3.8.4-7] - the SUBACK must report the granted max, not requested.
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 2u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16).to_io(io)
          io.flush

          suback = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::SubAck)
          suback.reason_codes.should eq([MQTT::Protocol::SubAck::ReasonCode::GrantedQoS1])
        end
      end
    end
  end
end
