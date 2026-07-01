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

      @server_capabilities : Protocol::ConnackProperties

      def initialize(@authenticator : Auth::Authenticator,
                     @brokers : Brokers, @config : Config)
        @server_capabilities = build_server_capabilities
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
        logger = Logger.new(Log, metadata)
        begin
          # CONNECT carries the protocol version on the wire, so bootstrap with a
          # v3 IO and reframe to the negotiated version (v3.1 / v3.1.1 / v5) for
          # every subsequent packet. Keeping the v3 boot IO in scope lets the
          # rescue below still answer a parse-time Connect error with a CONNACK.
          io = Protocol::IO::V3.new(socket, @config.mqtt_max_packet_size)
          if packet = io.read_packet.as?(Protocol::Connect)
            io = io.reframe(packet.version)
            logger.trace { "recv #{packet.inspect}" }
            user, broker = authenticate(io, packet)
            # A client that sends an empty client id gets one assigned; a v5
            # CONNACK must echo it back so the client learns its id [MQTT-3.2.2-16].
            assigned_client_id = nil
            if packet.client_id.empty?
              packet = with_assigned_client_id(packet, user.name)
              assigned_client_id = packet.client_id
            end
            validate_client_id!(packet.client_id, user.name)
            session_present = broker.session_present?(packet.client_id, packet.clean_session?)
            connack io, session_present, Protocol::Connack::ReturnCode::Accepted, assigned_client_id
            broker.run_client(io, connection_info, user, packet)
          end
        rescue ex : Protocol::Error::Connect
          logger.warn { "Connect error #{ex.inspect}" }
          if io
            connack io, false, Protocol::Connack::ReturnCode.new(ex.return_code)
          end
          socket.close
        rescue ::IO::EOFError
          socket.close
        rescue ex
          logger.warn { "Received invalid Connect packet: #{ex.inspect}" }
          socket.close
        end
      end

      private def connack(io : Protocol::IO, session_present : Bool,
                          return_code : Protocol::Connack::ReturnCode,
                          assigned_client_id : String? = nil)
        reason = Protocol::Connack::ReasonCode.from_v3_return_code(return_code)
        # A v5 server must advertise which optional features it supports; an
        # accepted v5 connection carries the capability set. On v3 the properties
        # are ignored on the wire, so the v3 CONNACK is byte-for-byte unchanged.
        properties =
          if io.version.v5? && return_code.accepted?
            if assigned_client_id
              # Per-connection, so build a fresh set rather than mutating the
              # shared static one.
              caps = build_server_capabilities
              caps.assigned_client_identifier = assigned_client_id
              caps
            else
              @server_capabilities
            end
          else
            Protocol::ConnackProperties.new
          end
        Protocol::Connack.new(session_present, reason, properties).to_io(io)
        io.flush
      end

      # The fixed v5 capabilities LavinMQ advertises in CONNACK. They depend only
      # on config (fixed after startup), so this is built once in initialize.
      # Advertising a feature as unavailable is what makes deferring it spec-
      # compliant; each deferred feature is then rejected in its own packet handler.
      private def build_server_capabilities : Protocol::ConnackProperties
        props = Protocol::ConnackProperties.new
        props.maximum_qos = 1u8       # QoS 2 not implemented
        props.retain_available = true # LavinMQ has a retain store
        props.wildcard_subscription_available = true
        props.topic_alias_maximum = 0u16                # topic aliases not implemented
        props.subscription_identifier_available = false # subscription ids not implemented
        props.shared_subscription_available = false     # shared subscriptions not implemented
        props.maximum_packet_size = @config.mqtt_max_packet_size
        props
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

      # Returns a copy of the CONNECT with a server-generated client id filled in
      # (Connect is an immutable struct, so the id can't be set in place). Used
      # when the client sends an empty client id.
      def with_assigned_client_id(packet, username : String)
        client_id = case @config.mqtt_client_id_validation
                    in .none?     then Random::Secure.base64(32)
                    in .username? then username
                    end
        # Preserve the negotiated version and the client's CONNECT properties;
        # only the client id changes.
        Protocol::Connect.new(client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.username,
          packet.password,
          packet.will,
          packet.version,
          packet.properties)
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
