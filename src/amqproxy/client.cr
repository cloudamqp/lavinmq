require "socket"

module AMQProxy
  class Client
    def initialize(@socket : TCPSocket)
      negotiate_client(@socket)
      @channel = Channel(AMQP::Frame?).new
      spawn decode_frames
    end

    def decode_frames
      loop do
        frame = AMQP::Frame.decode @socket
        case frame
        when AMQP::Connection::Close
          @socket.write AMQP::Connection::CloseOk.new.to_slice
          @channel.send nil
          break
        end
        @channel.send frame
      end
    rescue ex : IO::EOFError
      puts "Client conn closed"
      @channel.send nil
    end

    def next_frame
      @channel.receive_select_action
    end

    def write(bytes : Slice(UInt8))
      @socket.write bytes
    end

    private def negotiate_client(client)
      start = Bytes.new(8)
      bytes = client.read_fully(start)

      if start != AMQP::PROTOCOL_START
        client.write AMQP::PROTOCOL_START
        client.close
        return
      end

      start = AMQP::Connection::Start.new
      client.write start.to_slice

      start_ok = AMQP::Frame.decode client
      puts start_ok.inspect

      tune = AMQP::Connection::Tune.new(heartbeat: 60_u16)
      client.write tune.to_slice

      tune_ok = AMQP::Frame.decode client

      open = AMQP::Frame.decode client

      open_ok = AMQP::Connection::OpenOk.new
      client.write open_ok.to_slice
    end
  end

end

