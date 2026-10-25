module LavinMQ
  module HTTP
    # Accepted sockets inherit TCP_NODELAY from the listener
    def self.tcp_listener(address, port, & : TCPServer ->) : TCPServer
      tcp = TCPServer.new(address, port)
      tcp.tcp_nodelay = Config.instance.tcp_nodelay?
      yield tcp
      tcp
    rescue ex
      tcp.try &.close
      raise ex
    end
  end
end
