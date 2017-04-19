require "socket"
require "./amqp"
require "./token_bucket"
require "./pool"
require "./client"
require "./upstream"

module AMQProxy
  class Server
    def initialize(upstream_address : String, upstream_port : Int32)
      @pool = Pool(Upstream).new(1) do
        Upstream.new(upstream_address, upstream_port)
      end
    end

    def listen(port : Int)
      server = TCPServer.new("localhost", port)
      puts "Proxy listening on #{server.local_address}"
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

      #bucket = TokenBucket.new(100, 5.seconds)
      @pool.borrow do |upstream|
        begin
          loop do
            idx, frame = Channel.select([upstream.next_frame, client.next_frame])
            case idx
            when 0
              break if frame.nil?
              client.write frame.to_slice
            when 1
              if frame.nil?
                upstream.close_all_open_channels
                break
              else
                upstream.write frame.to_slice
              end
            end
          end
        rescue ex : IO::EOFError | Errno
          puts "Client loop #{ex.inspect}"
        ensure
          socket.close
        end
      end
    end
  end
end
