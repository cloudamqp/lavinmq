require "socket/address"

module LavinMQ
  module Auth
    struct Context
      getter username, password, is_ssl_login
      getter? loopback

      def self.new(username : String, password : Bytes, remote : ::IO)
        remote_address = case remote
                         when IPSocket   then remote.remote_address
                         when UNIXSocket then remote.remote_address
                         end
        self.new(username, password, remote_address)
      end

      def self.new(username : String, password : Bytes, address : ::Socket::Address?)
        loopback = case address
                   when Socket::IPAddress   then address.loopback?
                   when Socket::UNIXAddress then true
                   else                          false
                   end
        new(username, password, loopback: loopback)
      end

      def initialize(
        @username : String,
        @password : Bytes,
        *,
        @is_ssl_login : Bool = false,
        @loopback : Bool = false,
      )
      end
    end
  end
end
