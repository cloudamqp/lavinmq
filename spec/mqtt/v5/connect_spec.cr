require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 connect" do
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

    it "negotiates the protocol version from CONNECT and replies with a v5 CONNACK" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          # A v5 CONNECT must be answered with a v5-framed CONNACK; if the
          # broker kept v3 framing the reply would be unparseable here.
          connack = connect(io, version: MQTT::Protocol::Version::V5)
          connack.should be_a(MQTT::Protocol::Connack)
          connack = connack.as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
        end
      end
    end

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
        end
      end
    end

    it "closes without a CONNACK that exceeds the client's Maximum Packet Size [MQTT-3.1.2-24]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          props = MQTT::Protocol::ConnectProperties.new
          # Our CONNACK carries the capability set, so it is well over 5 bytes.
          props.maximum_packet_size = 5u32
          connect(io, version: MQTT::Protocol::Version::V5, client_id: "tiny",
            properties: props, expect_response: false)
          io.should be_closed
          # Refused before `run_client`, so no session was made either.
          server.vhosts["/"].session?("mqtt.tiny").should be_nil
        end
      end
    end

    it "echoes a server-assigned client id via assigned_client_identifier [MQTT-3.2.2-16]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "", clean_session: true).as(MQTT::Protocol::Connack)
          assigned = connack.properties.assigned_client_identifier
          assigned.should_not be_nil
          assigned = assigned.not_nil!
          assigned.should_not be_empty
          # The advertised id must be the one the broker actually registered.
          registered = wait_for do
            server.vhosts["/"].connections.select(LavinMQ::MQTT::Client).first?.try(&.client_id)
          end
          registered.should eq(assigned)
        end
      end
    end

    it "assigns a client id when Clean Start is 0 too (§3.1.3.1)" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          # v3.1.1 rejects an empty client id without Clean Session
          # [MQTT-3.1.3-8 v3.1.1]; v5 dropped that condition.
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "", clean_session: false).as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
          connack.properties.assigned_client_identifier.should_not be_nil
        end
      end
    end

    it "does not set assigned_client_identifier when the client supplies a client id" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO.v5(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5,
            client_id: "supplied-id").as(MQTT::Protocol::Connack)
          connack.properties.assigned_client_identifier.should be_nil
        end
      end
    end
  end
end
