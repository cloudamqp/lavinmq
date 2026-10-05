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
          # CONNECT carries the protocol version on the wire, so the IO starts
          # unpinned and read_connect switches its framing in place (v3.1 /
          # v3.1.1 / v5). The IO keeps its identity, so the rescue below answers
          # a failed CONNECT with a CONNACK framed for the version it asked for.
          io = Protocol::IO.new(socket, @config.mqtt_max_packet_size)
          packet = io.read_connect
          logger.trace { "recv #{packet.inspect}" }
          # Enhanced authentication (the AUTH-packet flow) is not supported;
          # reject before username/password auth so the reason is accurate. v5
          # only - v3 has no properties. [MQTT-4.12.0-1]
          if packet.properties.authentication_method
            logger.warn { "Enhanced authentication requested but not supported" }
            reject_connack(io, packet, Protocol::Connack::ReasonCode::BadAuthenticationMethod)
            return socket.close
          end
          user, broker = authenticate(packet, connection_info)
          # A client that sends an empty client id gets one assigned; a v5
          # CONNACK must echo it back so the client learns its id [MQTT-3.2.2-16].
          assigned_client_id = nil
          if packet.client_id.empty?
            assigned_client_id = generated_client_id(user.name)
            packet = packet.copy_with(client_id: assigned_client_id)
          end
          validate_client_id!(packet.client_id, user.name)
          if broker.connection_limit_reached?(packet.client_id)
            raise Protocol::Error::ServerUnavailable.new(
              "too many connections to vhost \"#{broker.vhost.name}\"")
          end
          properties = connack_properties(io, assigned_client_id)
          # Checked before `run_client`, so a client that cannot take our
          # CONNACK never gets a session. Session Present is a flag bit, so the
          # size does not depend on it.
          if too_large?(io, packet, Protocol::Connack.new(false, Protocol::Connack::ReasonCode::Success, properties))
            logger.warn { "CONNACK exceeds the client's Maximum Packet Size, closing" }
            return socket.close
          end
          broker.run_client(io, connection_info, user, packet) do |session_present|
            connack io, packet, session_present, Protocol::Connack::ReturnCode::Accepted, properties
          end
        rescue ex : Protocol::Error::Connect
          logger.warn { "Connect error #{ex.inspect}" }
          if io
            connack io, packet, false, Protocol::Connack::ReturnCode.new(ex.return_code)
          end
          socket.close
        rescue ::IO::EOFError
          socket.close
        rescue ex
          logger.warn { "Received invalid Connect packet: #{ex.inspect}" }
          socket.close
        end
      end

      # Send a v5 CONNACK carrying a reason code that has no v3 return-code
      # equivalent (e.g. BadAuthenticationMethod 0x8C). v5-only by construction.
      private def reject_connack(io : Protocol::IO, connect : Protocol::Connect,
                                 reason : Protocol::Connack::ReasonCode)
        write_connack(io, connect, Protocol::Connack.new(false, reason))
      end

      # `connect` is nil when the CONNECT itself failed to decode, and then
      # there is no Maximum Packet Size to honour.
      private def connack(io : Protocol::IO, connect : Protocol::Connect?, session_present : Bool,
                          return_code : Protocol::Connack::ReturnCode,
                          properties = Protocol::ConnackProperties.new)
        reason = Protocol::Connack::ReasonCode.from_v3_return_code(return_code)
        write_connack(io, connect, Protocol::Connack.new(session_present, reason, properties))
      end

      private def write_connack(io : Protocol::IO, connect : Protocol::Connect?, connack : Protocol::Connack)
        # Not sent at all rather than sent oversized [MQTT-3.1.2-24]; the
        # caller closes the socket either way.
        return if connect && too_large?(io, connect, connack)
        connack.to_io(io)
        io.flush
      end

      private def too_large?(io : Protocol::IO, connect : Protocol::Connect, packet) : Bool
        max = connect.properties.maximum_packet_size || return false
        io.bytesize(packet) > max
      end

      # A v5 server must advertise which optional features it supports; an
      # accepted v5 connection carries the capability set. On v3 the properties
      # are ignored on the wire, so the v3 CONNACK is byte-for-byte unchanged.
      private def connack_properties(io : Protocol::IO, assigned_client_id : String?) : Protocol::ConnackProperties
        return Protocol::ConnackProperties.new unless io.version.v5?
        return @server_capabilities unless assigned_client_id
        # Per-connection, so build a fresh set rather than mutating the shared
        # static one.
        caps = build_server_capabilities
        caps.assigned_client_identifier = assigned_client_id
        caps
      end

      # The fixed v5 capabilities LavinMQ advertises in CONNACK. They depend only
      # on config (fixed after startup), so this is built once in initialize.
      # Advertising a feature as unavailable is what makes deferring it spec-
      # compliant; each deferred feature is then rejected in its own packet handler.
      private def build_server_capabilities : Protocol::ConnackProperties
        props = Protocol::ConnackProperties.new
        props.retain_available = true # LavinMQ has a retain store
        props.wildcard_subscription_available = true
        props.topic_alias_maximum = 0u16                # topic aliases not implemented
        props.subscription_identifier_available = false # subscription ids not implemented
        props.shared_subscription_available = false     # shared subscriptions not implemented
        props.maximum_packet_size = @config.mqtt_max_packet_size
        props
      end

      def authenticate(packet, connection_info : ConnectionInfo)
        username = packet.username
        password = packet.password
        raise Protocol::Error::NotAuthorized.new("missing credentials") unless username && password

        vhost = @config.default_mqtt_vhost
        if split_pos = username.index(':')
          vhost = username[0, split_pos]
          username = username[split_pos + 1..]
        end

        context = Auth::Context.new(username, password, loopback: connection_info.loopback?)

        user = @authenticator.authenticate(context)
        raise Protocol::Error::NotAuthorized.new("authentication failure for user \"#{username}\"") unless user
        raise Protocol::Error::NotAuthorized.new("user \"#{username}\" lacks permission for vhost \"#{vhost}\"") unless user.find_permission(vhost)
        broker = @brokers[vhost]?
        raise Protocol::Error::NotAuthorized.new("no broker for vhost \"#{vhost}\"") unless broker

        {user, broker}
      end

      # A server-generated client id for a client that connected without one.
      private def generated_client_id(username : String) : String
        case @config.mqtt_client_id_validation
        in .none?     then Random::Secure.base64(32)
        in .username? then username
        end
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
