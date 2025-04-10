require "socket"

module LavinMQ
  class ConnectionInfo
    getter src : IPAddress
    getter dst : IPAddress
    property? ssl : Bool = false
    property? ssl_verify : Bool = false
    property ssl_version : String?
    property ssl_cipher : String?
    property ssl_key_alg : String?
    property ssl_sig_alg : String?
    property ssl_cn : String?

    def initialize(src, dst)
      @src = IPAddress.new(src)
      @dst = IPAddress.new(dst)
    end

    def self.local
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      new(src, dst)
    end

    def remote_address # from the server's perspective
      @src             # from the client's perspective
    end

    def local_address # from the server's perspective
      @dst            # from the client's perspective
    end

    # Suspecting memory problem with Socket::IPAddress in Crystal 1.15.0
    class IPAddress
      getter address : String
      getter port : UInt16

      def initialize(ip_address : Socket::IPAddress)
        @address = ip_address.address
        @port = ip_address.port.to_u16!
      end

      def to_s(io)
        io << @address << ':' << @port
      end

      def loopback?
        @address == "::1" || @address.starts_with? "127."
      end
    end
  end
end
