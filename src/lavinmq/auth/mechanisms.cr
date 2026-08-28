require "./mechanisms/plain"
require "./mechanisms/amqplain"
require "./mechanisms/external"

module LavinMQ
  module Auth
    module Mechanisms
      PLAIN    = Plain.new
      AMQPLAIN = AMQPlain.new
      EXTERNAL = External.new

      # Returns the mechanism handler for a SASL mechanism name.
      def self.[](name : String) : Mechanism
        case name
        when "PLAIN"    then PLAIN
        when "AMQPLAIN" then AMQPLAIN
        when "EXTERNAL" then EXTERNAL
        else                 raise "Unsupported authentication mechanism: #{name}"
        end
      end
    end
  end
end
