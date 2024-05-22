require "socket"

lib LibC
  TCP_QUICKACK = 12
end

class TCPSocket < IPSocket
  def tcp_quickack=(val : Bool)
    setsockopt_bool LibC::TCP_QUICKACK, val, level: Protocol::TCP
  end
end
