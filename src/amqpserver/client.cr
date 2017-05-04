require "socket"

module AMQPServer
  class Client
    def initialize(@socket : TCPSocket)
      @socket.sync = false
      @open_channels = Set(UInt16).new
      negotiate_client
    end

    def run_loop
      loop do
        frame = AMQP::Frame.decode @socket
        puts "=> #{frame.inspect}"
        case frame
        when AMQP::Channel::Open
          @open_channels.add frame.channel
          send AMQP::Channel::OpenOk.new(frame.channel)
        when AMQP::Channel::Close
          @open_channels.delete frame.channel
          send AMQP::Channel::CloseOk.new(frame.channel)
        when AMQP::Exchange::Declare
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        when AMQP::Connection::Close
          send AMQP::Connection::CloseOk.new
        end
      end
    end

    def send(frame : AMQP::Frame)
      puts "<= #{frame.inspect}"
      @socket.write frame.to_slice
      @socket.flush
    end

    private def negotiate_client
      start = Bytes.new(8)
      bytes = @socket.read_fully(start)

      if start != AMQP::PROTOCOL_START
        @socket.write AMQP::PROTOCOL_START
        @socket.close
        return
      end

      start = AMQP::Connection::Start.new
      send start
      start_ok = AMQP::Frame.decode @socket
      tune = AMQP::Connection::Tune.new(heartbeat: 0_u16)
      send tune
      tune_ok = AMQP::Frame.decode @socket
      open = AMQP::Frame.decode @socket
      open_ok = AMQP::Connection::OpenOk.new
      send open_ok
    end
  end
end
