require "socket"
require "logger"
require "./amqp"
require "./client"
require "./vhost"
require "./exchange"
require "./queue"

module AMQPServer
  class Server
    getter connections
    getter vhosts

    def initialize(data_dir : String, log_level = Logger::INFO)
      @log = Logger.new(STDOUT)
      @connections = Array(Client).new
      @conn_opened = Channel(Client).new
      @conn_closed = Channel(Client).new
      @vhosts = { "default" => VHost.new("default", data_dir, @log) }
      spawn handle_connection_events
    end

    def listen(port : Int)
      server = TCPServer.new("localhost", port)
      @log.info "Server listening on #{server.local_address}"
      loop do
        if socket = server.accept?
          spawn handle_connection(socket)
        else
          break
        end
      end
    end

    def close
      @connections.each { |c| c.close }
      @vhosts.each_value { |v| v.close }
    end

    private def handle_connection(socket)
      if client = Client.start(socket, @vhosts, @log)
        @conn_opened.send client
        client.on_close { |c| @conn_closed.send c }
      end
    end

    private def handle_connection_events
      loop do
        idx, conn = Channel.select(@conn_opened.receive_select_action,
                                   @conn_closed.receive_select_action)
        case idx
        when 0 # open
          @connections.push conn if conn
        when 1 # close
          @connections.delete conn if conn
        end
        @log.info "connection#count=#{@connections.size}"
      end
    end
  end
end
