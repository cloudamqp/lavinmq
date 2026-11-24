require "socket"

module LavinMQ
  module Auth
    struct Context
      getter username, password, mechanism
      getter? loopback

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
        @mechanism : String = "",
        @loopback : Bool = false,
      )
      end

      def external_authentication?
        @mechanism == "EXTERNAL"
      end
    end
  end
end
