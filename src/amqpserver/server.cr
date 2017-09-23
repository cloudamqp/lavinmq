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
      @conn_opened = Channel(Client).new
      @conn_closed = Channel(Client).new
      spawn handle_connection_events
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
        @conn_opened.send client
        client.on_close { |c| @conn_closed.send c }
      end
    end

    def handle_connection_events
      loop do
        idx, conn = Channel.select(@conn_opened.receive_select_action,
                                   @conn_closed.receive_select_action)
        case idx
        when 0 # open
          @connections.push conn if conn
        when 1 # close
          @connections.delete conn if conn
        end
        print "connection#count=", @connections.size, "\n"
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
            "q1" => Queue.new("q1", durable: true, auto_delete: false, exclusive: false, arguments: {} of String => AMQP::Field)
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
