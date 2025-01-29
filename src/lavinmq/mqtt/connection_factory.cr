require "log"
require "socket"
require "./protocol"
require "./client"
require "./brokers"
require "../user"
require "../client/connection_factory"

module LavinMQ
  module MQTT
    class ConnectionFactory < LavinMQ::ConnectionFactory
      Log = LavinMQ::Log.for "mqtt.connection_factory"

      def initialize(@users : UserStore,
                     @brokers : Brokers, @config : Config)
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        remote_address = connection_info.src
        metadata = ::Log::Metadata.build({address: remote_address.to_s})
        logger = Logger.new(Log, metadata)
        begin
          io = MQTT::IO.new(socket)
          if packet = Packet.from_io(socket).as?(Connect)
            logger.trace { "recv #{packet.inspect}" }
            if user_and_broker = authenticate(io, packet)
              user, broker = user_and_broker
              packet = assign_client_id(packet) if packet.client_id.empty?
              session_present = broker.session_present?(packet.client_id, packet.clean_session?)
              connack io, session_present, Connack::ReturnCode::Accepted
              return broker.add_client(socket, connection_info, user, packet)
            else
              logger.warn { "Authentication failure for user \"#{packet.username}\"" }
              connack io, false, Connack::ReturnCode::NotAuthorized
            end
          end
        rescue ex : MQTT::Error::Connect
          logger.warn { "Connect error #{ex.inspect}" }
          if io
            connack io, false, Connack::ReturnCode.new(ex.return_code)
          end
          socket.close
        rescue ex
          logger.warn { "Recieved invalid Connect packet: #{ex.inspect}" }
          socket.close
        end
      end

      private def connack(io : MQTT::IO, session_present : Bool, return_code : Connack::ReturnCode)
        Connack.new(session_present, return_code).to_io(io)
        io.flush
      end

      def authenticate(io, packet)
        return unless (username = packet.username) && (password = packet.password)

        if split_pos = username.index(':')
          @config.default_mqtt_vhost = username[0, split_pos]
          username = username[split_pos + 1..]
        end

        user = @users[username]?
        return unless user
        return unless user.password && user.password.try(&.verify(String.new(password)))
        has_vhost_permissions = user.try &.permissions.has_key?(@config.default_mqtt_vhost)
        return unless has_vhost_permissions
        broker = @brokers[@config.default_mqtt_vhost]?
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
