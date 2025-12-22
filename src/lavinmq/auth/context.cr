require "socket/address"

module LavinMQ
  module Auth
    struct Context
      getter username, password
      getter? loopback

      def self.new(
        username : String,
        password : Bytes,
        remote : ::IO,
      )
        loopback = case remote
                   when IPSocket   then remote.remote_address.loopback?
                   when UNIXSocket then true
                   else                 false
                   end

        new(username, password, loopback: loopback)
      end

      def self.new(
        username : String,
        password : Bytes,
        address : ::Socket::Address?,
      )
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
        @loopback : Bool = false,
      )
      end
    end
  end
end
