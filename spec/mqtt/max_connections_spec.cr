require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT max-connections" do
    it "refuses connections over the vhost limit" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_connections = 1
        with_client_io(server) do |io|
          connect(io, client_id: "c1")
          wait_for { vhost.connections_size == 1 }
          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "c2").should be_a(MQTT::Protocol::Connack)
            connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::ServerUnavailable
            io2.should be_closed
          end
        end
      end
    end

    it "accepts connections again once the limit is lifted" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_connections = 1
        with_client_io(server) do |io|
          connect(io, client_id: "c1")
          wait_for { vhost.connections_size == 1 }
          vhost.max_connections = -1
          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "c2").should be_a(MQTT::Protocol::Connack)
            connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
          end
        end
      end
    end

    # The limit used to be checked before the same client_id takeover in
    # Broker#add_client, so a reconnecting client could never get back in
    it "lets a client with the same client_id reconnect at the limit" do
      with_server do |server|
        vhost = server.vhosts["/"]
        vhost.max_connections = 1
        with_client_io(server) do |io|
          connect(io, client_id: "c1")
          wait_for { vhost.connections_size == 1 }
          with_client_io(server) do |io2|
            connack = connect(io2, client_id: "c1").should be_a(MQTT::Protocol::Connack)
            connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
            io.should be_closed
          end
        end
      end
    end
  end
end
