require "socket"

module LavinMQ
  class ConnectionInfo
    getter remote_address : Address
    getter local_address : Address
    property? ssl : Bool = false
    property? ssl_verify : Bool = false
    property ssl_version : String?
    property ssl_cipher : String?
    property ssl_key_alg : String?
    property ssl_sig_alg : String?
    property ssl_cn : String?

    # Remote and local addresses from the server's perspective
    def initialize(remote_address, local_address)
      @remote_address = Address.new(remote_address)
      @local_address = Address.new(local_address)
    end

    # Suspecting memory problem with Socket::IPAddress in Crystal 1.15.0
    struct Address
      getter address : String
      getter port : UInt16

      def initialize(ip_address : Socket::IPAddress)
        @address = ip_address.address
        @port = ip_address.port.to_u16!
      end

      def initialize(address : String)
        if address.starts_with? "/"
          @address = address
          @port = 0
          return
        end
        parts = address.split(":")
        if parts.size != 2
          raise "Socket address should be host:port"
        end
        @address = parts[0]
        @port = parts[1].to_u16
      end

      def to_s(io)
        io << @address << ':' << @port
      end

      def loopback?
        @address == "::1" || @address.starts_with?("127.") || @address.starts_with?("/")
      end
    end
  end
end
