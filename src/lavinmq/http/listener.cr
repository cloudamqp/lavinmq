module LavinMQ
  module HTTP
    # Accepted sockets inherit TCP_NODELAY and TCP_NOTSENT_LOWAT from the
    # listener. The latter keeps tcp_send_timeout from closing WebSocket
    # clients that read slowly, also under TLS.
    def self.tcp_listener(address, port, & : TCPServer ->) : TCPServer
      tcp = TCPServer.new(address, port)
      tcp.tcp_nodelay = Config.instance.tcp_nodelay?
      tcp.tcp_notsent_lowat = Config::TCP_NOTSENT_LOWAT
      yield tcp
      tcp
    rescue ex
      tcp.try &.close
      raise ex
    end
  end
end
