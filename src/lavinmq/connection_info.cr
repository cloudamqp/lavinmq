require "socket"

module LavinMQ
  class ConnectionInfo
    getter remote_address : IPAddress
    getter local_address : IPAddress
    property? ssl : Bool = false
    property? ssl_verify : Bool = false
    property ssl_version : String?
    property ssl_cipher : String?
    property ssl_key_alg : String?
    property ssl_sig_alg : String?
    property ssl_cn : String?
    property ssl_san : String?

    # Remote and local addresses from the server's perspective
    def initialize(remote_address, local_address)
      @remote_address = IPAddress.new(remote_address)
      @local_address = IPAddress.new(local_address)
    end

    def self.local
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      new(src, dst)
    end

    # Suspecting memory problem with Socket::IPAddress in Crystal 1.15.0
    struct IPAddress
      getter address : String
      getter port : UInt16
      getter? loopback : Bool

      def initialize(ip_address : Socket::IPAddress)
        @address = ip_address.address
        @port = ip_address.port.to_u16!
        @loopback = ip_address.loopback?
      end

      def to_s(io)
        io << @address << ':' << @port
      end
    end
  end
end
