require "log"
require "socket"
require "./protocol"
require "./client"
require "./brokers"
require "../auth/base_user"
require "../client/connection_factory"
require "../auth/authenticator"
require "../auth_failure_tracker"

module LavinMQ
  module MQTT
    class ConnectionFactory < LavinMQ::ConnectionFactory
      Log = LavinMQ::Log.for "mqtt.connection_factory"

      def initialize(@authenticator : Auth::Authenticator,
                     @brokers : Brokers, @config : Config)
        @auth_failure_tracker = AuthFailureTracker.new
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
        logger = Logger.new(Log, metadata)
        begin
          io = MQTT::IO.new(socket, @config.mqtt_max_packet_size)
          if packet = io.read_packet.as?(Connect)
            logger.trace { "recv #{packet.inspect}" }
            if user_and_broker = authenticate(io, packet)
              user, broker = user_and_broker
              packet = assign_client_id(packet) if packet.client_id.empty?
              session_present = broker.session_present?(packet.client_id, packet.clean_session?)
              connack io, session_present, Connack::ReturnCode::Accepted
              return broker.add_client(io, connection_info, user, packet)
            else
              ip = connection_info.remote_address.address
              username = packet.username || ""
              @auth_failure_tracker.track(username, ip) do |suppressed|
                if suppressed > 0
                  logger.warn { "Authentication failure for user \"#{username}\" (repeated #{suppressed} times in the last #{AuthFailureTracker::SUPPRESSION_WINDOW.total_seconds.to_i}s)" }
                else
                  logger.warn { "Authentication failure for user \"#{username}\"" }
                end
              end
              connack io, false, Connack::ReturnCode::NotAuthorized
            end
          end
        rescue ex : MQTT::Error::Connect
          logger.warn { "Connect error #{ex.inspect}" }
          if io
            connack io, false, Connack::ReturnCode.new(ex.return_code)
          end
          socket.close
        rescue ex : ::IO::EOFError
          socket.close
        rescue ex
          logger.warn { "Received invalid Connect packet: #{ex.inspect}" }
          socket.close
        end
      end

      private def connack(io : MQTT::IO, session_present : Bool, return_code : Connack::ReturnCode)
        Connack.new(session_present, return_code).to_io(io)
        io.flush
      end

      def authenticate(io : MQTT::IO, packet)
        return unless (username = packet.username) && (password = packet.password)

        vhost = @config.default_mqtt_vhost
        if split_pos = username.index(':')
          vhost = username[0, split_pos]
          username = username[split_pos + 1..]
        end

        context = Auth::Context.new(username, password, io.io)

        user = @authenticator.authenticate(context)
        return unless user
        return unless user.find_permission(vhost)
        broker = @brokers[vhost]?
        return unless broker

        {user, broker}
      end

      def assign_client_id(packet)
        client_id = Random::Secure.base64(32)
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
