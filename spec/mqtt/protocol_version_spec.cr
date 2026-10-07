require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  describe "MQTT protocol version" do
    it "reports MQTT 3.1.1 for protocol level 4" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, version: 4u8)
          conn = wait_for { server.connections.first?.as?(LavinMQ::MQTT::Client) }
          conn.details_tuple[:protocol].should eq "MQTT 3.1.1"
        end
      end
    end

    it "reports MQTT 3.1 for protocol level 3 (MQIsdp)" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, version: 3u8)
          conn = wait_for { server.connections.first?.as?(LavinMQ::MQTT::Client) }
          conn.details_tuple[:protocol].should eq "MQTT 3.1"
        end
      end
    end

    it "keeps the negotiated version when the client id is auto-assigned" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, version: 3u8, client_id: "", clean_session: true)
          conn = wait_for { server.connections.first?.as?(LavinMQ::MQTT::Client) }
          conn.details_tuple[:protocol].should eq "MQTT 3.1"
        end
      end
    end
  end
end
