require "log"
require "socket"
require "./protocol"
require "./client"
require "./brokers"
require "../auth/base_user"
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
        metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
        logger = Logger.new(Log, metadata)
        begin
          io = Protocol::IO.new(socket, @config.mqtt_max_packet_size)
          if packet = io.read_packet.as?(Protocol::Connect)
            logger.trace { "recv #{packet.inspect}" }
            user, broker = authenticate(io, packet)
            packet = assign_client_id(packet, user.name) if packet.client_id.empty?
            validate_client_id!(packet.client_id, user.name)
            session_present = broker.session_present?(packet.client_id, packet.clean_session?)
            connack io, session_present, Protocol::Connack::ReturnCode::Accepted
            return broker.run_client(io, connection_info, user, packet)
          end
        rescue ex : Protocol::Error::Connect
          logger.warn { "Connect error #{ex.inspect}" }
          if io
            connack io, false, Protocol::Connack::ReturnCode.new(ex.return_code)
          end
          socket.close
        rescue ex : ::IO::EOFError
          socket.close
        rescue ex
          logger.warn { "Received invalid Connect packet: #{ex.inspect}" }
          socket.close
        end
      end

      private def connack(io : Protocol::IO, session_present : Bool, return_code : Protocol::Connack::ReturnCode)
        Protocol::Connack.new(session_present, return_code).to_io(io)
        io.flush
      end

      def authenticate(io : Protocol::IO, packet)
        username = packet.username
        password = packet.password
        raise Protocol::Error::NotAuthorized.new("missing credentials") unless username && password

        vhost = @config.default_mqtt_vhost
        if split_pos = username.index(':')
          vhost = username[0, split_pos]
          username = username[split_pos + 1..]
        end

        context = Auth::Context.new(username, password, io.io)

        user = @authenticator.authenticate(context)
        raise Protocol::Error::NotAuthorized.new("authentication failure for user \"#{username}\"") unless user
        raise Protocol::Error::NotAuthorized.new("user \"#{username}\" lacks permission for vhost \"#{vhost}\"") unless user.find_permission(vhost)
        broker = @brokers[vhost]?
        raise Protocol::Error::NotAuthorized.new("no broker for vhost \"#{vhost}\"") unless broker

        {user, broker}
      end

      def assign_client_id(packet, username : String)
        client_id = case @config.mqtt_client_id_validation
                    in .none?     then Random::Secure.base64(32)
                    in .username? then username
                    end
        Protocol::Connect.new(client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.username,
          packet.password,
          packet.will)
      end

      private def validate_client_id!(client_id : String, username : String) : Nil
        case @config.mqtt_client_id_validation
        in .none?
          return
        in .username?
          return if client_id == username
          raise Protocol::Error::IdentifierRejected.new(
            %(client_id "#{client_id}" rejected: it must be the same as the username "#{username}"))
        end
      end
    end
  end
end
