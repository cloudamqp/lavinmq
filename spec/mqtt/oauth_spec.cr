require "./spec_helper"
require "../oauth_spec"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "MQTT OAuth" do
    it "allows connection with wildcard vhost permissions" do
      with_server do |server|
        reader, writer = IO.pipe
        mqtt_io = MQTT::Protocol::IO.new(reader)
        conn_info = LavinMQ::ConnectionInfo.local

        # OAuthUser with wildcard vhost "*" should match default vhost "/"
        permissions = {"*" => {config: /.*/, read: /.*/, write: /.*/}}
        user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

        broker = server.@mqtt_brokers.@brokers["/"]
        packet = MQTT::Protocol::Connect.new(
          client_id: "oauth-wildcard-test",
          clean_session: true,
          keepalive: 60u16,
          username: "testuser",
          password: nil,
          will: nil,
        )

        broker.add_client(mqtt_io, conn_info, user, packet)

        sleep 100.milliseconds

        # Client should be connected
        reader.closed?.should be_false
      ensure
        reader.try &.close
        writer.try &.close
      end
    end

    it "allows connection with exact vhost permissions" do
      with_server do |server|
        reader, writer = IO.pipe
        mqtt_io = MQTT::Protocol::IO.new(reader)
        conn_info = LavinMQ::ConnectionInfo.local

        permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
        user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

        broker = server.@mqtt_brokers.@brokers["/"]
        packet = MQTT::Protocol::Connect.new(
          client_id: "oauth-exact-test",
          clean_session: true,
          keepalive: 60u16,
          username: "testuser",
          password: nil,
          will: nil,
        )

        broker.add_client(mqtt_io, conn_info, user, packet)

        sleep 100.milliseconds

        reader.closed?.should be_false
      ensure
        reader.try &.close
        writer.try &.close
      end
    end

    it "cleans up OAuthUser on disconnect" do
      with_server do |server|
        reader, writer = IO.pipe
        mqtt_io = MQTT::Protocol::IO.new(reader)
        conn_info = LavinMQ::ConnectionInfo.local

        permissions = {"/" => {config: /.*/, read: /.*/, write: /.*/}}
        user = OAuthUserHelper.create_user(RoughTime.utc + 1.hour, permissions)

        broker = server.@mqtt_brokers.@brokers["/"]
        packet = MQTT::Protocol::Connect.new(
          client_id: "oauth-cleanup-test",
          clean_session: true,
          keepalive: 60u16,
          username: "testuser",
          password: nil,
          will: nil,
        )

        broker.add_client(mqtt_io, conn_info, user, packet)

        # Close the socket to trigger disconnect and cleanup
        reader.close
        sleep 200.milliseconds

        # The OAuthUser's token_updated channel should be closed by cleanup
        user.@token_updated.closed?.should be_true
      ensure
        writer.try &.close
      end
    end
  end
end
