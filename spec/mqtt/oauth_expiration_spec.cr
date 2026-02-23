require "./spec_helper"
require "../oauth_spec"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT OAuth token expiration" do
    it "disconnects client when token expires" do
      with_server do |server|
        # Create a pipe pair to simulate the MQTT socket
        reader, writer = IO.pipe
        mqtt_io = MQTT::Protocol::IO.new(reader)
        conn_info = LavinMQ::ConnectionInfo.local

        # Create OAuthUser with a very short-lived token
        permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
        user = OAuthUserHelper.create_user(RoughTime.utc + 1.second, permissions)

        broker = server.@mqtt_brokers.@brokers["/"]
        packet = MQTT::Protocol::Connect.new(
          client_id: "oauth-expiry-test",
          clean_session: true,
          keepalive: 60u16,
          username: "testuser",
          password: nil,
          will: nil,
        )

        broker.add_client(mqtt_io, conn_info, user, packet)

        # Client should be connected
        reader.closed?.should be_false

        # Wait for token to expire and the on_expiration callback to fire
        sleep 1.5.seconds

        # Client should be disconnected after token expiry
        reader.closed?.should be_true
      ensure
        writer.try &.close
      end
    end

    it "does not disconnect client when token is still valid" do
      with_server do |server|
        reader, writer = IO.pipe
        mqtt_io = MQTT::Protocol::IO.new(reader)
        conn_info = LavinMQ::ConnectionInfo.local

        # Create OAuthUser with a long-lived token
        permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
        user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

        broker = server.@mqtt_brokers.@brokers["/"]
        packet = MQTT::Protocol::Connect.new(
          client_id: "oauth-valid-test",
          clean_session: true,
          keepalive: 60u16,
          username: "testuser",
          password: nil,
          will: nil,
        )

        broker.add_client(mqtt_io, conn_info, user, packet)

        sleep 500.milliseconds

        # Client should still be connected
        reader.closed?.should be_false
      ensure
        reader.try &.close
        writer.try &.close
      end
    end
  end
end
