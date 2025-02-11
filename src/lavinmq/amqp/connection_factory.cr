require "../version"
require "../logger"
require "./client"
require "../user_store"
require "../vhost_store"
require "../client/connection_factory"
require "../auth/authenticator"

module LavinMQ
  module AMQP
    class ConnectionFactory < LavinMQ::ConnectionFactory
      Log = LavinMQ::Log.for "amqp.connection_factory"

      def initialize(@authenticator : Auth::Authenticator, @vhosts : VHostStore)
      end

      def start(socket, connection_info) : Client?
        remote_address = connection_info.src
        socket.read_timeout = 15.seconds
        metadata = ::Log::Metadata.build({address: remote_address.to_s})
        logger = Logger.new(Log, metadata)
        if confirm_header(socket, logger)
          if start_ok = start(socket, logger)
            if user = authenticate(socket, remote_address, start_ok, logger)
              if tune_ok = tune(socket, logger)
                if vhost = open(socket, user, logger)
                  socket.read_timeout = heartbeat_timeout(tune_ok)
                  return LavinMQ::AMQP::Client.new(socket, connection_info, vhost, user, tune_ok, start_ok)
                end
              end
            end
          end
        end
      rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
        Log.warn { "#{ex} when #{remote_address} tried to establish connection" }
        nil
      rescue ex
        Log.error(exception: ex) { "Error while #{remote_address} tried to establish connection" }
        nil
      end

      private def heartbeat_timeout(tune_ok)
        if tune_ok.heartbeat > 0
          (tune_ok.heartbeat / 2).seconds
        end
      end

      def confirm_header(socket, log : Logger) : Bool
        proto = uninitialized UInt8[8]
        count = socket.read(proto.to_slice)
        if count.zero? # EOF, socket closed by peer
          false
        elsif proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
          socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
          socket.flush
          log.warn { "Unexpected protocol #{String.new(proto.to_unsafe, count).inspect}, closing socket" }
          false
        else
          true
        end
      end

      SERVER_PROPERTIES = AMQP::Table.new({
        "product":      "LavinMQ",
        "platform":     "Crystal #{Crystal::VERSION}",
        "version":      LavinMQ::VERSION,
        "capabilities": {
          "publisher_confirms":           true,
          "exchange_exchange_bindings":   true,
          "basic.nack":                   true,
          "consumer_cancel_notify":       true,
          "connection.blocked":           true,
          "consumer_priorities":          true,
          "authentication_failure_close": true,
          "per_consumer_qos":             true,
          "direct_reply_to":              true,
        },
      })

      def start(socket, log : Logger)
        start = AMQP::Frame::Connection::Start.new(server_properties: SERVER_PROPERTIES)
        socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
        socket.flush
        start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
        if start_ok.bytesize > 4096
          log.warn { "StartOk frame was #{start_ok.bytesize} bytes, max allowed is 4096 bytes" }
          return
        end
        start_ok
      end

      def credentials(start_ok)
        case start_ok.mechanism
        when "PLAIN"
          resp = start_ok.response
          if i = resp.index('\u0000', 1)
            {resp[1...i], resp[(i + 1)..-1]}
          else
            raise "Invalid authentication response"
          end
        when "AMQPLAIN"
          io = ::IO::Memory.new(start_ok.response)
          tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.bytesize.to_u32)
          {tbl["LOGIN"].as(String), tbl["PASSWORD"].as(String)}
        else raise "Unsupported authentication mechanism: #{start_ok.mechanism}"
        end
      end

      def authenticate(socket, remote_address, start_ok, log)
        username, password = credentials(start_ok)
        user = @authenticator.authenticate(username, password)
        return user if user && guest_only_loopback?(remote_address, user)
        if user.nil?
          log.warn { "User \"#{username}\" not found" }
        else
          log.warn { "Authentication failure for user \"#{username}\"" }
        end
        props = start_ok.client_properties
        if capabilities = props["capabilities"]?.try &.as?(AMQP::Table)
          if capabilities["authentication_failure_close"]?.try &.as?(Bool)
            close_connection(socket, Client::ConnectionReplyCode::ACCESS_REFUSED, "", start_ok)
          end
        end
        nil
      end

      MIN_FRAME_MAX       = 4096_u32
      LOW_FRAME_MAX_RANGE = 1...MIN_FRAME_MAX # 0 is unlimited

      def tune(socket, log)
        frame_max = socket.is_a?(WebSocketIO) ? MIN_FRAME_MAX : Config.instance.frame_max
        socket.write_bytes AMQP::Frame::Connection::Tune.new(
          channel_max: Config.instance.channel_max,
          frame_max: frame_max,
          heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
        socket.flush
        tune_ok = AMQP::Frame.from_io(socket) do |frame|
          case frame
          when AMQP::Frame::Connection::TuneOk
            if frame.frame_max < 4096
              log.warn { "Suggested Frame max (#{frame.frame_max}) too low, closing connection" }
              return
            end
            frame
          else
            log.warn { "Expected TuneOk Frame got #{frame.inspect}" }
            return
          end
        end
        if tune_ok.frame_max < 4096
          log.warn { "Suggested Frame max (#{tune_ok.frame_max}) too low, closing connection" }
          return
        end
        tune_ok
      end

      def open(socket, user, log)
        open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
        vhost_name = open.vhost.empty? ? "/" : open.vhost
        if vhost = @vhosts[vhost_name]?
          if user.permissions[vhost_name]?
            if vhost.max_connections.try { |max| vhost.connections.size >= max }
              log.warn { "Max connections (#{vhost.max_connections}) reached for vhost #{vhost_name}" }
              reply_text = "access to vhost '#{vhost_name}' refused: connection limit (#{vhost.max_connections}) is reached"
              return close_connection(socket, Client::ConnectionReplyCode::NOT_ALLOWED, reply_text, open)
            end
            socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
            socket.flush
            return vhost
          else
            log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
            reply_text = "'#{user.name}' doesn't have access to '#{vhost.name}'"
            close_connection(socket, Client::ConnectionReplyCode::NOT_ALLOWED, reply_text, open)
          end
        else
          log.warn { "VHost \"#{vhost_name}\" not found" }
          close_connection(socket, Client::ConnectionReplyCode::NOT_ALLOWED, "vhost not found", open)
        end
        nil
      end

      private def guest_only_loopback?(remote_address, user) : Bool
        return true unless user.name == "guest"
        return true unless Config.instance.guest_only_loopback?
        remote_address.loopback?
      end

      private def close_connection(socket, code : Client::ConnectionReplyCode, text, frame)
        text = "#{code} - #{text}"
        socket.write_bytes(
          AMQP::Frame::Connection::Close.new(
            code.value,
            text,
            frame.class_id,
            frame.method_id),
          IO::ByteFormat::NetworkEndian)
        socket.flush
        nil
      end
    end
  end
end
