require "socket"
require "openssl"

module AMQProxy
  class Upstream
    def initialize(@host : String, @port : Int32,
                   @user : String, @password : String, @vhost : String,
                   @tls : Bool)

      @socket = uninitialized IO
      @frame_channel = Channel(AMQP::Frame?).new
      @open_channels = Set(UInt16).new
      connect!
    end

    def connect!
      tcp_socket = TCPSocket.new(@host, @port)
      @socket = if @tls
                  #raise "TLS not implemented yet"
                  context = OpenSSL::SSL::Context::Client.new
                  @socket = OpenSSL::SSL::Socket::Client.new(tcp_socket, context)
                else
                  tcp_socket
                end
      negotiate_server
      spawn decode_frames
    end

    def decode_frames
      loop do
        frame = AMQP::Frame.decode @socket
        case frame
        when AMQP::Channel::OpenOk
          #puts "Channeled open ok #{frame.channel}"
          @open_channels.add frame.channel
        when AMQP::Channel::CloseOk
          #puts "Channeled close ok #{frame.channel}"
          @open_channels.delete frame.channel
        end
        @frame_channel.send frame
      end
    rescue ex : Errno | IO::EOFError
      puts "proxy decode frame: #{ex.inspect}"
      raise ex
    ensure
      puts "proxy decode_frames ended"
    end

    def next_frame
      @frame_channel.receive_select_action
    end

    def write(bytes : Slice(UInt8))
      @socket.write bytes
    end

    def closed?
      @socket.closed?
    end

    def close_all_open_channels
      @open_channels.each do |ch|
        puts "Closing client channel #{ch}"
        @socket.write AMQP::Channel::Close.new(ch, 200_u16, "", 0_u16, 0_u16).to_slice
        @frame_channel.receive
      end
    end

    private def negotiate_server
      @socket.write AMQP::PROTOCOL_START

      start = AMQP::Frame.decode @socket
      assert_frame_type start, AMQP::Connection::Start

      start_ok = AMQP::Connection::StartOk.new(response: "\u0000#{@user}\u0000#{@password}")
      @socket.write start_ok.to_slice

      tune = AMQP::Frame.decode @socket
      assert_frame_type tune, AMQP::Connection::Tune

      channel_max = tune.as(AMQP::Connection::Tune).channel_max
      tune_ok = AMQP::Connection::TuneOk.new(heartbeat: 0_u16, channel_max: channel_max)
      @socket.write tune_ok.to_slice

      open = AMQP::Connection::Open.new(vhost: @vhost)
      @socket.write open.to_slice

      open_ok = AMQP::Frame.decode @socket
      assert_frame_type open_ok, AMQP::Connection::OpenOk
    end

    private def assert_frame_type(frame, clz)
      unless frame.class == clz
        raise "Expected frame #{clz} but got: #{frame.inspect}"
      end
    end
  end
end
