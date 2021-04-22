require "socket"

module AvalancheMQ
  struct ConnectionInfo
    getter src : Socket::IPAddress
    getter dst : Socket::IPAddress
    property ssl : Bool = false
    property ssl_verify : Bool = false
    property ssl_version : String?
    property ssl_cipher : String?
    property ssl_key_alg : String?
    property ssl_sig_alg : String?
    property ssl_cn : String?

    def initialize(@src, @dst)
    end

    def self.local
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      new(src, dst)
    end
  end
end
