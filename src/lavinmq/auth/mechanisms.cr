require "./mechanisms/plain"
require "./mechanisms/amqplain"
require "./mechanisms/external"

module LavinMQ
  module Auth
    module Mechanisms
      PLAIN    = Plain.new
      AMQPLAIN = AMQPlain.new
      EXTERNAL = External.new

      DEFAULT_MECHANISMS       = "AMQPLAIN PLAIN"
      MECHANISMS_WITH_EXTERNAL = "AMQPLAIN PLAIN EXTERNAL"

      # Returns the mechanism handler for a SASL mechanism name.
      def self.[](name : String) : Mechanism
        case name
        when "PLAIN"    then PLAIN
        when "AMQPLAIN" then AMQPLAIN
        when "EXTERNAL" then EXTERNAL
        else                 raise Error.new("Unsupported authentication mechanism: #{name}")
        end
      end

      # The space separated mechanisms offered to a client in Connection.Start.
      # EXTERNAL is only offered when it can succeed on this connection.
      def self.advertised(connection_info : ConnectionInfo) : String
        External.available?(connection_info) ? MECHANISMS_WITH_EXTERNAL : DEFAULT_MECHANISMS
      end
    end
  end
end
