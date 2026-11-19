require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "with keepalive" do
    it "client is disconnected after 1.5 * [keep alive] seconds", tags: "slow" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false, keepalive: 1u16)
          io.io.as(TCPSocket).read_timeout = 10.seconds
          elapsed = Time.measure { io.should be_closed }
          elapsed.should be < 2.5.seconds
        end
      end
    end
  end
end
