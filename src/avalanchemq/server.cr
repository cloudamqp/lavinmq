require "socket"
require "logger"
require "./amqp"
require "./client"
require "./vhost"
require "./exchange"
require "./queue"

module AvalancheMQ
  class Server
    getter connections
    getter vhosts

    def initialize(data_dir : String, log_level)
      @log = Logger.new(STDOUT)
      @log.level = log_level
      @log.progname = "AMQP Server"
      @log.formatter = Logger::Formatter.new do |severity, datetime, progname, message, io|
        io << progname << ": " << message
      end
      @listeners = Array(TCPServer).new(1)
      @connections = Array(Client).new
      @conn_opened = Channel(Client).new
      @conn_closed = Channel(Client).new
      @vhosts = { "default" => VHost.new("default", data_dir, @log) }
      spawn handle_connection_events, name: "Server#handle_connection_events"
    end

    def listen(port : Int)
      s = TCPServer.new("::", port)
      @listeners << s
      @log.info "Listening on #{s.local_address}"
      loop do
        if socket = s.accept?
          handle_connection(socket)
        else
          break
        end
      end
    ensure
      @listeners.delete(s)
    end

    def close
      @log.debug "Closing listeners"
      @listeners.each { |l| l.close }
      @log.debug "Closing connections"
      @connections.each { |c| c.close }
      @log.debug "Closing vhosts"
      @vhosts.each_value { |v| v.close }
    end

    private def handle_connection(socket)
      socket.keepalive = true
      socket.tcp_nodelay = false
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.linger = 0
      if client = Client.start(socket, @vhosts, @log)
        @conn_opened.send client
        client.on_close { |c| @conn_closed.send c }
      else
        socket.close
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
        @log.debug "connection#count=#{@connections.size}"
      end
    end
  end
end
