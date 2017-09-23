require "socket"
require "./amqp"
require "./client"
require "./message"
require "./exchange"
require "./queue"

module AMQPServer
  class Server
    def initialize
      @state = State.new
      @connections = Array(Client).new
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
      if client = Client.start(socket, @state)
        @connections.push client
      end
    end

    class State
      getter vhosts
      def initialize
        @vhosts = { "default" => VHost.new("default") }
      end

      class VHost
        getter name, exchanges, queues

        def initialize(@name : String)
          @queues = {
            "q1" => Queue.new("q1")
          }
          @exchanges = {
            "" => Exchange.new("", type: "direct", durable: true,
                               arguments: {} of String => AMQP::Field,
                               bindings: { "q1" => [@queues["q1"]] })
          }
        end
      end
    end
  end
end
