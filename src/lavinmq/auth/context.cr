require "socket"

module LavinMQ
  module Auth
    struct Context
      getter username, password, vhost
      getter? loopback

      def self.new(username : String, password : Bytes, address : ::Socket::Address?, vhost : String? = nil)
        loopback = case address
                   when Socket::IPAddress   then address.loopback?
                   when Socket::UNIXAddress then true
                   else                          false
                   end
        new(username, password, loopback: loopback, vhost: vhost)
      end

      # `vhost` is the vhost the client is connecting to, if known at
      # authentication time (AMQP and MQTT). Used to look up vhost scoped users.
      def initialize(
        @username : String,
        @password : Bytes,
        *,
        @loopback : Bool = false,
        @vhost : String? = nil,
      )
      end
    end
  end
end
