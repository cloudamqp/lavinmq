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
      params = ::HTTP::Params.parse(@uri.query.to_s)
      heartbeat = params["heartbeat"]?.try(&.to_u16?) || 0_u16
      connect_timeout = params["connection_timeout"]?.try(&.to_u16?)
      channel_max = params["channel_max"]?.try(&.to_u16?) || 1_u16
      auth_mechanism = params["auth_mechanism"]?.try { |v| v =~ /AMQPLAIN/i } ? "AMQPLAIN" : "PLAIN"
      port = @uri.port || (tls ? 5671 : 5672)
      @log.debug { "Connecting on #{port} with #{@uri.scheme}" }
      socket = tcp_socket(host, port, connect_timeout)
      @socket = tls ? tls_socket(socket, params, host) : socket
      negotiate_connection(channel_max, heartbeat, auth_mechanism)
      open_channel
    end

    private def tcp_socket(host, port, connect_timeout)
      socket = TCPSocket.new(host, port, connect_timeout: connect_timeout)
      socket.keepalive = true
      socket.tcp_nodelay = false
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.write_timeout = 15
      socket
    end

    private def tls_socket(socket, params, host)
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
      OpenSSL::SSL::Socket::Client.new(socket, context: context, sync_close: true, hostname: host)
    end

    def negotiate_connection(channel_max, heartbeat, auth_mechanism)
      @socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
      @socket.flush
      AMQP::Frame.from_io(@socket) { |f| f.as?(AMQP::Frame::Connection::Start) || raise UnexpectedFrame.new(f) }

      props = AMQP::Table.new(Hash(String, AMQP::Field).new)
      user = URI.decode_www_form(@uri.user || "guest")
      password = URI.decode_www_form(@uri.password || "guest")
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
      vhost = path.size > 1 ? URI.decode_www_form(path[1..-1]) : "/"
      write AMQP::Frame::Connection::Open.new(vhost)
      AMQP::Frame.from_io(@socket) { |f| f.as?(AMQP::Frame::Connection::OpenOk) || raise UnexpectedFrame.new(f) }
    end

    def open_channel
      write AMQP::Frame::Channel::Open.new(1_u16)
      AMQP::Frame.from_io(@socket) { |f| f.as?(AMQP::Frame::Channel::OpenOk) || raise UnexpectedFrame.new(f) }
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
