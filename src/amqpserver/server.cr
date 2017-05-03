require "socket"
require "./amqp"
require "./token_bucket"
require "./client"
require "./backend"

module AMQPServer
  class Server
    def listen(port : Int)
      server = TCPServer.new("localhost", port)
      puts "Server listening on #{server.local_address}"
      loop do
        if socket = server.accept?
          spawn handle_connection(socket)
        else
          break
        end
      end
    end

    def handle_connection(socket)
      client = Client.new(socket)
      puts "Client connection opened"

      backend = Backend.new
      #bucket = TokenBucket.new(100, 5.seconds)
      loop do
        idx, frame = Channel.select([backend.next_frame, client.next_frame])
        break if frame.nil?
        case idx
        when 0
          puts "<= #{frame.inspect}"
          client.write frame.to_slice
        when 1
          puts "=> #{frame.inspect}"
          backend.process_frame frame
        end
      end
    rescue ex : IO::EOFError | Errno
      puts "Client loop #{ex.inspect}"
    ensure
      puts "Client connection closed"
      socket.close
    end
  end
end
