require "log"
require "socket"
require "./protocol"
require "./client"
require "./brokers"
require "../auth/user"
require "../client/connection_factory"
require "../auth/authenticator"

module LavinMQ
  module MQTT
    class ConnectionFactory < LavinMQ::ConnectionFactory
      Log = LavinMQ::Log.for "mqtt.connection_factory"

      def initialize(@authenticator : Auth::Authenticator,
                     @brokers : Brokers, @config : Config)
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        # Create temporary logger context for this connection attempt
        io = MQTT::IO.new(socket)
        if packet = Packet.from_io(socket).as?(Connect)
          # TODO: Replace with L.trace once this method includes Loggable
          if user_and_broker = authenticate(io, packet)
            user, broker = user_and_broker
            packet = assign_client_id(packet) if packet.client_id.empty?
            session_present = broker.session_present?(packet.client_id, packet.clean_session?)
            connack io, session_present, Connack::ReturnCode::Accepted
            return broker.add_client(socket, connection_info, user, packet)
          else
            # TODO: Replace with L.warn once this method includes Loggable
            connack io, false, Connack::ReturnCode::NotAuthorized
          end
        end
      rescue ex : MQTT::Error::Connect
        # TODO: Replace with L.warn once this method includes Loggable
        if io
          connack io, false, Connack::ReturnCode.new(ex.return_code)
        end
        socket.close
      rescue ex : ::IO::EOFError
        socket.close
      rescue ex
        # TODO: Replace with L.warn once this method includes Loggable
        socket.close
      end

      private def connack(io : MQTT::IO, session_present : Bool, return_code : Connack::ReturnCode)
        Connack.new(session_present, return_code).to_io(io)
        io.flush
      end

      def authenticate(io, packet)
        return unless (username = packet.username) && (password = packet.password)

        vhost = @config.default_mqtt_vhost
        if split_pos = username.index(':')
          vhost = username[0, split_pos]
          username = username[split_pos + 1..]
        end

        user = @authenticator.authenticate(username, password)
        return unless user
        has_vhost_permissions = user.try &.permissions.has_key?(vhost)
        return unless has_vhost_permissions
        broker = @brokers[vhost]?
        return unless broker

        {user, broker}
      end

      def assign_client_id(packet)
        client_id = Random::DEFAULT.base64(32)
        Connect.new(client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.username,
          packet.password,
          packet.will)
      end
    end
  end
end
