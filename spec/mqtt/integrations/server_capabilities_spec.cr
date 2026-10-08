require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  # The compliance contract in MQTT5.md section 2: every optional feature we
  # leave out is advertised as unavailable in CONNACK and rejected when used.
  describe "MQTT 5.0 server capabilities" do
    it "advertises server capabilities in the v5 CONNACK" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5).as(MQTT::Protocol::Connack)
          props = connack.properties
          # Absent means 2, and 2 may not be sent (3.2.2.3.4).
          props.maximum_qos?.should be_nil
          props.retain_available?.should be_true
          props.wildcard_subscription_available?.should be_true
          props.topic_alias_maximum.should eq(0u16)
          props.subscription_identifier_available?.should be_false
          props.shared_subscription_available?.should be_false
          props.maximum_packet_size.should eq(LavinMQ::Config.instance.mqtt_max_packet_size)
          props.receive_maximum.should eq(LavinMQ::Config.instance.max_awaiting_pubrel)
        end
      end
    end
    it "rejects enhanced authentication with BadAuthenticationMethod (0x8C)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          # A CONNECT carrying an Authentication Method wants the AUTH-packet
          # flow, which we don't support -> CONNACK 0x8C [MQTT-4.12.0-1].
          props = MQTT::Protocol::ConnectProperties.new
          props.authentication_method = "SCRAM-SHA-1"
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            properties: props).as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::BadAuthenticationMethod)
        end
      end
    end
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
    it "disconnects with TopicAliasInvalid (0x94) when a client sends a Topic Alias" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised topic_alias_maximum=0, so any Topic Alias is invalid.
          props = MQTT::Protocol::PublishProperties.new
          props.topic_alias = 1u16
          MQTT::Protocol::Publish.new(
            topic: "test/topic", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 1u8, retain: false, properties: props,
          ).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::TopicAliasInvalid)
        end
      end
    end
  end
end
