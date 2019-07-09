require "uri"
require "logger"
require "socket"
require "openssl"
require "http"
require "./amqp"

module AvalancheMQ
  abstract class Connection
    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @verify_mode = OpenSSL::SSL::VerifyMode::PEER

    getter verify_mode

    def initialize(@uri : URI, @log : Logger)
      host = @uri.host || "localhost"
      tls = @uri.scheme == "amqps"
      channel_max = 1_u16
      heartbeat = 0_u16
      connect_timeout = nil
      auth_mechanism = "PLAIN"
      params = ::HTTP::Params.parse(@uri.query.to_s)
      params.each do |key, value|
        case key
        when "heartbeat"
          heartbeat = value.to_u16? || heartbeat
        when "connection_timeout"
          connect_timeout = value.to_u16?
        when "channel_max"
          channel_max = value.to_u16? || channel_max
        when "auth_mechanism"
          auth_mechanism = "AMQPLAIN" if value =~ /AMQPLAIN/i
        end
      end
      port = @uri.port || (tls ? 5671 : 5672)
      @log.debug { "Connecting on #{port} with #{@uri.scheme}" }
      socket = TCPSocket.new(host, port, connect_timeout: connect_timeout)
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.write_timeout = 15

      if tls
        context = OpenSSL::SSL::Context::Client.new
        params.each do |key, value|
          case key
          when "verify"
            case value
            when "none"
              @verify_mode = OpenSSL::SSL::VerifyMode::NONE
              context.verify_mode = @verify_mode
            end
          when "cacertfile"
            context.ca_certificates = value
          when "certfile"
            context.certificate_chain = value
          when "keyfile"
            context.private_key = value
          end
        end
        @socket = OpenSSL::SSL::Socket::Client.new(socket, context: context,
          sync_close: true, hostname: host)
      else
        @socket = socket
      end
      negotiate_connection(channel_max, heartbeat, auth_mechanism)
      open_channel
    end

    def negotiate_connection(channel_max, heartbeat, auth_mechanism)
      @socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
      @socket.flush
      AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::Start) }

      props = AMQP::Table.new(Hash(String, AMQP::Field).new)
      user = URI.unescape(@uri.user || "guest")
      password = URI.unescape(@uri.password || "guest")
      if auth_mechanism == "AMQPLAIN"
        tbl = AMQP::Table.new({
          "LOGIN"    => user,
          "PASSWORD" => password,
        } of String => AMQP::Field)
        response = String.new(tbl.buffer, tbl.bytesize - 4)
      else
        response = "\u0000#{user}\u0000#{password}"
      end
      write AMQP::Frame::Connection::StartOk.new(props, auth_mechanism, response, "")
      AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::Tune) }
      write AMQP::Frame::Connection::TuneOk.new(channel_max: channel_max,
        frame_max: 131072_u32, heartbeat: heartbeat)
      path = @uri.path || ""
      vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
      write AMQP::Frame::Connection::Open.new(vhost)
      AMQP::Frame.from_io(@socket, IO::ByteFormat::NetworkEndian) { |f| f.as(AMQP::Frame::Connection::OpenOk) }
    end

    def open_channel
      write AMQP::Frame::Channel::Open.new(1_u16)
      AMQP::Frame.from_io(@socket) { |f| f.as(AMQP::Frame::Channel::OpenOk) }
    end

    def close(msg = "Connection closed")
      return if @socket.closed?
      write AMQP::Frame::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
    rescue Errno
      @log.info("Socket already closed, can't send close frame")
    end

    def write(frame)
      return if @socket.closed?
      @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
      @socket.flush
    end

    def closed?
      @socket.closed?
    end

    class UnexpectedFrame < Exception
      def initialize(@frame : AMQP::Frame)
        msg = @frame.class.name
        if frame.is_a?(AMQP::Frame::Channel::Close)
          msg += ": " + frame.as(AMQP::Frame::Channel::Close).reply_text
        elsif frame.is_a?(AMQP::Frame::Connection::Close)
          msg += ": " + frame.as(AMQP::Frame::Connection::Close).reply_text
        end
        super(msg)
      end
    end
  end
end
