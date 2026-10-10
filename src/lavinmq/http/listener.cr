module LavinMQ
  module HTTP
    # Accepted sockets inherit TCP_NODELAY from the listener
    def self.tcp_listener(address, port) : TCPServer
      TCPServer.new(address, port).tap(&.tcp_nodelay = Config.instance.tcp_nodelay?)
    end
  end
end
