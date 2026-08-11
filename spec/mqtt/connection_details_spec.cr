require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  describe "MQTT connection details" do
    it "exposes connection details" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, client_id: "my_client", keepalive: 45u16)
          conn = wait_for { server.connections.first?.as?(LavinMQ::MQTT::Client) }
          details = conn.details_tuple

          details[:protocol].should eq "MQTT 3.1.1"
          details[:vhost].should eq "/"
          details[:user].should eq "guest"
          details[:client_id].should eq "my_client"
          details[:name].should contain " -> "
          details[:state].should eq "running"
          details[:timeout].should eq 45

          details[:host].should_not be_empty
          details[:port].should be > 0
          details[:peer_host].should_not be_empty
          details[:peer_port].should be > 0
          conn.connection_info.remote_address.loopback?.should be_true

          details[:ssl].should be_false
          details[:tls_version].should be_nil
          details[:cipher].should be_nil
        end
      end
    end
  end
end
