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
      socket.sync = true
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.write_timeout = 15
      socket.recv_buffer_size = 131072
      @socket =
        if tls
          OpenSSL::SSL::Socket::Client.new(socket, sync_close: true, hostname: host)
        else
          socket
        end
      negotiate_connection
      open_channel
    end

    def negotiate_connection
      @socket.write AMQP::PROTOCOL_START.to_slice
      start = AMQP::Frame.decode(@socket).as(AMQP::Connection::Start)

      props = {} of String => AMQP::Field
      user = URI.unescape(@uri.user || "guest")
      password = URI.unescape(@uri.password || "guest")
      response = "\u0000#{user}\u0000#{password}"
      start_ok = AMQP::Connection::StartOk.new(props, "PLAIN", response, "")
      @socket.write start_ok.to_slice
      tune = AMQP::Frame.decode(@socket).as(AMQP::Connection::Tune)
      @socket.write AMQP::Connection::TuneOk.new(channel_max: 1_u16,
        frame_max: 4096_u32,
        heartbeat: 0_u16).to_slice
      path = @uri.path || ""
      vhost = path.size > 1 ? URI.unescape(path[1..-1]) : "/"
      @socket.write AMQP::Connection::Open.new(vhost).to_slice
      frame = AMQP::Frame.decode(@socket)
      raise UnexpectedFrame.new(frame) unless frame.is_a?(AMQP::Connection::OpenOk)
    end

    def open_channel
      @socket.write AMQP::Channel::Open.new(1_u16).to_slice
      AMQP::Frame.decode(@socket).as(AMQP::Channel::OpenOk)
    end

    def force_close(msg = "Connection closed")
      return if @socket.closed?
      @socket.write AMQP::Connection::Close.new(320_u16, msg, 0_u16, 0_u16).to_slice
    end

    class UnexpectedFrame < Exception
      def initialize(@frame : AMQP::Frame)
        super(@frame.class.name)
      end
    end
  end
end
