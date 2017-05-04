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
      client.run_loop
    end
  end
end
