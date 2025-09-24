require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "with keepalive" do
    it "client is disconnected after 1.5 * [keep alive] seconds" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false, keepalive: 1u16)
          sleep 1.6.seconds
          io.should be_closed
        end
      end
    end
  end
end
