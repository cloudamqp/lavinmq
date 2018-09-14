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
      socket = TCPSocket.new(host, @uri.port || tls ? 5671 : 5672)
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.write_timeout = 15
      socket.recv_buffer_size = 131072
      if tls
        context = OpenSSL::SSL::Context::Client.new
        HTTP::Params.parse(@uri.query.to_s) do |key, value|
          case key
          when "verify"
            case value
            when "none"
              context.verify_mode = OpenSSL::SSL::VerifyMode::None
            end
          end
        end
        @socket = OpenSSL::SSL::Socket::Client.new(socket, context: context,
          sync_close: true, hostname: host)
      else
        @socket = socket
      end
      negotiate_connection
      open_channel
    end

    def negotiate_connection
      @socket.write AMQP::PROTOCOL_START.to_slice
      @socket.flush
      start = AMQP::Frame.decode(@socket).as(AMQP::Connection::Start)

      props = {} of String => AMQP::Field
      user = URI.unescape(@uri.user || "guest")
      password = URI.unescape(@uri.password || "guest")
      response = "\u0000#{user}\u0000#{password}"
      write AMQP::Connection::StartOk.new(props, "PLAIN", response, "")
      tune = AMQP::Frame.decode(@socket).as(AMQP::Connection::Tune)
      write AMQP::Connection::TuneOk.new(channel_max: 1_u16,
        frame_max: 131072_u32, heartbeat: 0_u16)
      path = @uri.path || ""
      vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
      write AMQP::Connection::Open.new(vhost)
      frame = AMQP::Frame.decode(@socket)
      raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Connection::OpenOk)
    end

    def open_channel
      write AMQP::Channel::Open.new(1_u16)
      AMQP::Frame.decode(@socket).as(AMQP::Channel::OpenOk)
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
