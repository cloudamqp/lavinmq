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

      def self.[](name : String) : Mechanism
        case name
        when "PLAIN"    then PLAIN
        when "AMQPLAIN" then AMQPLAIN
        when "EXTERNAL" then EXTERNAL
        else                 raise Error.new("Unsupported authentication mechanism: #{name}")
        end
      end

      def self.advertised(connection_info : ConnectionInfo) : String
        External.available?(connection_info) ? MECHANISMS_WITH_EXTERNAL : DEFAULT_MECHANISMS
      end
    end
  end
end
