require "../logger"
require "../auth/authenticator"
require "../vhost_store"
require "./frame"
require "./frames"
require "./sasl"
require "./client"

module LavinMQ
  module AMQP10
    # Establishes an AMQP 1.0 connection: SASL handshake (PLAIN/ANONYMOUS) then
    # the AMQP `open` exchange. Invoked from `AMQP::ConnectionFactory#confirm_header`
    # when a 1.0 protocol header arrives on the shared AMQP port.
    class ConnectionFactory
      Log = LavinMQ::Log.for "amqp10.connection_factory"

      def initialize(@authenticator : Auth::Authenticator, @vhosts : VHostStore)
      end

      # `header` is the 8 protocol-header bytes already consumed by the caller.
      def start(socket, connection_info, header : Bytes) : LavinMQ::Client?
        metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
        logger = Logger.new(Log, metadata)
        user = nil
        # protocol-id 3 == SASL layer first; 0 == raw AMQP (no SASL).
        if header[4] == 3_u8
          user = sasl_handshake(socket, connection_info, logger)
          return unless user
          # Client now sends the AMQP-layer protocol header.
          read_protocol_header(socket) || return
        end
        socket.write AMQP_PROTOCOL_HEADER
        socket.flush
        open(socket, connection_info, user, logger)
      rescue ex : IO::Error | Error
        Log.warn { "#{ex} while #{connection_info.remote_address} established AMQP 1.0 connection" }
        nil
      end

      private def read_protocol_header(socket) : Bool
        buf = uninitialized UInt8[8]
        socket.read_fully(buf.to_slice)
        true
      rescue IO::Error
        false
      end

      private def sasl_handshake(socket, connection_info, log) : Auth::BaseUser?
        socket.write SASL_PROTOCOL_HEADER
        socket.flush
        # offer mechanisms
        FrameWriter.write(socket, type: Frame::TYPE_SASL) { |b| Sasl::Mechanisms.new.to_io(b) }
        socket.flush
        frame = Frame.read(socket)
        perf = frame.performative
        return unless perf && perf.descriptor.as?(UInt64) == Descriptor::SASL_INIT
        init = Sasl::Init.decode(perf)
        user = authenticate(init, connection_info, log)
        code = user ? Sasl::OK : Sasl::AUTH
        FrameWriter.write(socket, type: Frame::TYPE_SASL) { |b| Sasl::Outcome.new(code).to_io(b) }
        socket.flush
        user
      end

      private def authenticate(init : Sasl::Init, connection_info, log) : Auth::BaseUser?
        case init.mechanism
        when "PLAIN"
          response = init.initial_response
          return unless response
          username, password = Sasl.plain_credentials(response)
          ctx = Auth::Context.new(username, password.to_slice, loopback: connection_info.remote_address.loopback?)
          if user = @authenticator.authenticate(ctx)
            return user
          end
          log.info { "Authentication failure for user \"#{username}\"" }
          nil
        else
          log.warn { "Unsupported SASL mechanism: #{init.mechanism}" }
          nil
        end
      end

      private def open(socket, connection_info, user : Auth::BaseUser?, log) : LavinMQ::Client?
        frame = Frame.read(socket)
        perf = frame.performative
        return unless perf && perf.descriptor.as?(UInt64) == Descriptor::OPEN
        open = Open.decode(perf)
        vhost_name = open.hostname.try { |h| h.empty? ? "/" : h } || "/"
        # SASL-less connections aren't authenticated; require SASL for now.
        unless user
          log.warn { "AMQP 1.0 connection without SASL is not supported" }
          return
        end
        vhost = @vhosts[vhost_name]?
        unless vhost && user.find_permission(vhost_name)
          log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
          send_open(socket, open) # still answer so client sees the close reason
          return
        end
        send_open(socket, open)
        max_frame = open.max_frame_size
        Client.new(socket, connection_info, vhost, user, open.container_id, max_frame, open.idle_timeout)
      end

      private def send_open(socket, remote_open : Open) : Nil
        our_open = Open.new(container_id: "LavinMQ", max_frame_size: Config.instance.frame_max,
          channel_max: UInt16::MAX, idle_timeout: nil)
        FrameWriter.write(socket) { |b| our_open.to_io(b) }
        socket.flush
      end
    end
  end
end
