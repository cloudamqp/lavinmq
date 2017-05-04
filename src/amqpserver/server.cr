require "socket"
require "./amqp"
require "./token_bucket"
require "./client"
require "./backend"

module AMQPServer
  class Server
    def initialize
      @state = State.new
    end

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
      client = Client.new(socket, @state)
      client.run_loop
    end

    class State
      getter exchanges

      def initialize
        @exchanges = {
          "amq.direct" => {
            type: "direct",
            durable: true,
            bindings: {
              "rk" => [
                Queue.new("q1")
              ]
            }
          }
        }
      end
    end

    class Queue
      class QueueFile < File
        include AMQP::IO

        def finalize
          flush
          close
          super
        end
      end

      def initialize(@name : String)
        @file = QueueFile.open("/tmp/#{@name}.q", "a")
        @next_msg_size = 0_u64
        @msg_bytes_left = 0_u64
      end
      
      def write_head(exchange_name : String, routing_key : String)
        @file.write_short_string exchange_name
        @file.write_short_string routing_key
      end

      def write_body_size(size : UInt64)
        @file.write_int size
        @msg_bytes_left = size
      end

      def write_content(bytes : Bytes)
        @file.write bytes
        @msg_bytes_left -= bytes.size
        @file.flush if @msg_bytes_left == 0
      end
    end
  end
end
