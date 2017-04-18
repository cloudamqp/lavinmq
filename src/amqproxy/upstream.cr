require "socket"

module AMQProxy
  class Upstream
    def initialize(@upstream_address : String, @upstream_port : Int32)
      @socket = TCPSocket.new(@upstream_address, @upstream_port)
      negotiate_server(@socket)

      @channel = Channel(AMQP::Frame?).new
      spawn decode_frames

      @open_channels = Set(UInt16).new
    end

    def decode_frames
      loop do
        frame = AMQP::Frame.decode @socket
        case frame
        when AMQP::Channel::OpenOk
          puts "Channeled open ok #{frame.channel}"
          @open_channels.add frame.channel
        when AMQP::Channel::CloseOk
          puts "Channeled close ok #{frame.channel}"
          @open_channels.delete frame.channel
        end
        @channel.send frame
      end
    rescue ex : Errno | IO::EOFError
      puts "proxy decode frame: #{ex.inspect}"
      raise ex
    ensure
      puts "proxy decode_frames ended"
    end

    def next_frame
      @channel.receive_select_action
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
        @channel.receive
      end
    end

    private def negotiate_server(server)
      server.write AMQP::PROTOCOL_START

      start = AMQP::Frame.decode server

      start_ok = AMQP::Connection::StartOk.new
      server.write start_ok.to_slice

      tune = AMQP::Frame.decode server

      tune_ok = AMQP::Connection::TuneOk.new(heartbeat: 0_u16)
      server.write tune_ok.to_slice

      open = AMQP::Connection::Open.new
      server.write open.to_slice

      open_ok = AMQP::Frame.decode server
    end
  end
end
