require "uri"
require "logger"
require "socket"
require "openssl"
require "./amqp"

module AvalancheMQ
  abstract class Connection
    @socket : TCPSocket | OpenSSL::SSL::Socket::Client

    def initialize(@uri : URI, @log : Logger)
      host = @uri.host || "localhost"
      tls = @uri.scheme == "amqps"
      channel_max = 1_u16
      heartbeat = 0_u16
      connect_timeout = nil
      auth_mechanism = "PLAIN"
      params = HTTP::Params.parse(@uri.query.to_s)
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
      port = @uri.port || tls ? 5671 : 5672
      socket = TCPSocket.new(host, port, connect_timeout: connect_timeout)
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.write_timeout = 15
      socket.recv_buffer_size = 131072

      if tls
        context = OpenSSL::SSL::Context::Client.new
        params.each do |key, value|
          case key
          when "verify"
            case value
            when "none"
              context.verify_mode = OpenSSL::SSL::VerifyMode::None
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
      @buffer = IO::Memory.new
      negotiate_connection(channel_max, heartbeat, auth_mechanism)
      open_channel
    end

    def negotiate_connection(channel_max, heartbeat, auth_mechanism)
      @socket.write AMQP::PROTOCOL_START.to_slice
      @socket.flush
      AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Connection::Start) }

      props = {} of String => AMQP::Field
      user = URI.unescape(@uri.user || "guest")
      password = URI.unescape(@uri.password || "guest")
      if auth_mechanism == "AMQPLAIN"
        tbl = AMQP::Table.new({
          "LOGIN"    => user,
          "PASSWORD" => password,
        } of String => AMQP::Field)
        io = IO::Memory.new
        tbl.to_io(io, ::IO::ByteFormat::NetworkEndian)
        response = io.to_s
      else
        response = "\u0000#{user}\u0000#{password}"
      end
      write AMQP::Connection::StartOk.new(props, auth_mechanism, response, "")
      AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Connection::Tune) }
      write AMQP::Connection::TuneOk.new(channel_max: channel_max,
        frame_max: 131072_u32, heartbeat: heartbeat)
      path = @uri.path || ""
      vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
      write AMQP::Connection::Open.new(vhost)
      AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Connection::OpenOk) }
    end

    def open_channel
      write AMQP::Channel::Open.new(1_u16)
      AMQP::Frame.decode(@socket, @buffer) { |f| f.as(AMQP::Channel::OpenOk) }
    end

    def close(msg = "Connection closed")
      return if @socket.closed?
      write AMQP::Connection::Close.new(320_u16, msg, 0_u16, 0_u16)
    rescue Errno
      @log.info("Socket already closed, can't send close frame")
    end

    def write(frame)
      @socket.write(frame.to_slice)
      @socket.flush
    end

    def closed?
      @socket.closed?
    end

    class UnexpectedFrame < Exception
      def initialize(@frame : AMQP::Frame)
        msg = @frame.class.name
        if frame.is_a?(AMQP::Channel::Close)
          msg += ": " + frame.as(AMQP::Channel::Close).reply_text
        elsif frame.is_a?(AMQP::Connection::Close)
          msg += ": " + frame.as(AMQP::Connection::Close).reply_text
        end
        super(msg)
      end
    end
  end
end
