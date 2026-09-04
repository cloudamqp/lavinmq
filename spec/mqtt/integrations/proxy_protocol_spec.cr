require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  private def self.with_proxy_protocol(&)
    config = LavinMQ::Config.instance
    previous_loopback = config.default_user_only_loopback?
    previous_proxy = config.tcp_proxy_protocol?
    config.default_user_only_loopback = true
    config.tcp_proxy_protocol = true
    yield
  ensure
    config = LavinMQ::Config.instance
    config.default_user_only_loopback = previous_loopback.nil? ? true : previous_loopback
    config.tcp_proxy_protocol = previous_proxy.nil? ? false : previous_proxy
  end

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
