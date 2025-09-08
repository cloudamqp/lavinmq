require "../version"
require "../logging"
require "./client"
require "../auth/user_store"
require "../vhost_store"
require "../client/connection_factory"
require "../auth/authenticator"
require "./connection_reply_code"

module LavinMQ
  module AMQP
    class ConnectionFactory < LavinMQ::ConnectionFactory
      include LavinMQ::Logging::Loggable
      Log = LavinMQ::Log.for "amqp.connection_factory"

      def initialize(@authenticator : Auth::Authenticator, @vhosts : VHostStore)
      end

      def start(socket, connection_info) : Client?
        socket.read_timeout = 15.seconds
        L.context(address: connection_info.remote_address)
        if confirm_header(socket)
          if start_ok = start(socket)
            if user = authenticate(socket, connection_info.remote_address, start_ok)
              if tune_ok = tune(socket)
                if vhost = open(socket, user)
                  socket.read_timeout = heartbeat_timeout(tune_ok)
                  return LavinMQ::AMQP::Client.new(socket, connection_info, vhost, user, tune_ok, start_ok)
                end
              end
            end
          end
        end
      rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | AMQ::Protocol::Error::FrameDecode
        L.warn "Connection establishment failed", remote_address: connection_info.remote_address, exception: ex
        nil
      rescue ex
        L.error "Connection establishment error", remote_address: connection_info.remote_address, exception: ex
        nil
      end

      private def heartbeat_timeout(tune_ok)
        if tune_ok.heartbeat > 0
          (tune_ok.heartbeat / 2).seconds
        end
      end

      def confirm_header(socket) : Bool
        proto = uninitialized UInt8[8]
        count = socket.read(proto.to_slice)
        if count.zero? # EOF, socket closed by peer
          false
        elsif proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
          socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
          socket.flush
          L.warn "Unexpected protocol, closing socket", protocol: String.new(proto.to_unsafe, count).inspect
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

      def start(socket)
        start = AMQP::Frame::Connection::Start.new(server_properties: SERVER_PROPERTIES)
        socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
        socket.flush
        start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
        if start_ok.bytesize > 4096
          L.warn "StartOk frame too large", size: start_ok.bytesize, max_allowed: 4096
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

      def authenticate(socket, remote_address, start_ok)
        username, password = credentials(start_ok)
        user = @authenticator.authenticate(username, password)
        return user if user && default_user_only_loopback?(remote_address, user)

        if user.nil?
          L.warn "User not found", username: username
        else
          L.warn "Authentication failure", username: username
        end
        props = start_ok.client_properties
        if capabilities = props["capabilities"]?.try &.as?(AMQP::Table)
          if capabilities["authentication_failure_close"]?.try &.as?(Bool)
            close_connection(socket, ConnectionReplyCode::ACCESS_REFUSED, "", start_ok)
          end
        end
        nil
      end

      MIN_FRAME_MAX       = 4096_u32
      LOW_FRAME_MAX_RANGE = 1...MIN_FRAME_MAX # 0 is unlimited

      def tune(socket)
        frame_max = socket.is_a?(WebSocketIO) ? MIN_FRAME_MAX : Config.instance.frame_max
        socket.write_bytes AMQP::Frame::Connection::Tune.new(
          channel_max: Config.instance.channel_max,
          frame_max: frame_max,
          heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
        socket.flush
        tune_ok = AMQP::Frame.from_io(socket) do |frame|
          case frame
          when AMQP::Frame::Connection::TuneOk
            if LOW_FRAME_MAX_RANGE.includes? frame.frame_max
              L.warn "Suggested Frame max too low, closing connection", frame_max: frame.frame_max
              reply_text = "failed to negotiate connection parameters: negotiated frame_max = #{frame.frame_max} is lower than the minimum allowed value (#{MIN_FRAME_MAX})"
              return close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, reply_text, frame)
            end
            if too_high?(frame.frame_max, Config.instance.frame_max)
              L.warn "Suggested Frame max too high, closing connection", frame_max: frame.frame_max
              reply_text = "failed to negotiate connection parameters: negotiated frame_max = #{frame.frame_max} is higher than the maximum allowed value (#{Config.instance.frame_max})"
              return close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, reply_text, frame)
            end
            if too_high?(frame.channel_max, Config.instance.channel_max)
              L.warn "Suggested Channel max too high, closing connection", channel_max: frame.channel_max
              reply_text = "failed to negotiate connection parameters: negotiated channel_max = #{frame.channel_max} is higher than the maximum allowed value (#{Config.instance.channel_max})"
              return close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, reply_text, frame)
            end
            frame
          else
            L.warn "Expected TuneOk Frame", got: frame.inspect
            return
          end
        end
        tune_ok
      end

      def open(socket, user)
        open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
        vhost_name = open.vhost.empty? ? "/" : open.vhost
        if vhost = @vhosts[vhost_name]?
          if user.permissions[vhost_name]?
            if vhost.max_connections.try { |max| vhost.connections.size >= max }
              L.warn "Max connections reached for vhost", max_connections: vhost.max_connections, vhost: vhost_name
              reply_text = "access to vhost '#{vhost_name}' refused: connection limit (#{vhost.max_connections}) is reached"
              return close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, reply_text, open)
            end
            socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
            socket.flush
            return vhost
          else
            L.warn "Access denied", user: user.name, vhost: vhost_name
            reply_text = "'#{user.name}' doesn't have access to '#{vhost.name}'"
            close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, reply_text, open)
          end
        else
          L.warn "VHost not found", vhost: vhost_name
          close_connection(socket, ConnectionReplyCode::NOT_ALLOWED, "vhost not found", open)
        end
        nil
      end

      private def default_user_only_loopback?(remote_address, user) : Bool
        return true unless user.name == Config.instance.default_user
        return true unless Config.instance.default_user_only_loopback?
        remote_address.loopback?
      end

      private def close_connection(socket, code : ConnectionReplyCode, text, frame)
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

      # zero is unlimited
      private def too_high?(client_value, server_value)
        (client_value.zero? && server_value.positive?) ||
          (server_value.positive? && client_value > server_value)
      end
    end
  end
end
