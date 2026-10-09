require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT default user loopback gate with PROXY protocol" do
    it "rejects the default user when a PROXY header claims a loopback source" do
      with_server do |server|
        with_proxy_protocol do
          with_client_socket(server) do |socket|
            socket.write "PROXY TCP4 127.0.0.1 127.0.0.1 54321 1883\r\n".to_slice
            io = MQTT::Protocol::IO.new(socket)
            connack = connect(io)
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq(MQTT::Protocol::Connack::ReturnCode::NotAuthorized)
          end
        end
      end
    end

    it "accepts the default user from a real loopback connection without a PROXY header" do
      with_server do |server|
        with_proxy_protocol do
          with_client_io(server) do |io|
            connack = connect(io)
            connack.should be_a(MQTT::Protocol::Connack)
            connack.as(MQTT::Protocol::Connack).return_code.should eq(MQTT::Protocol::Connack::ReturnCode::Accepted)
            disconnect(io)
          end
        end
      end
    end
  end
end
