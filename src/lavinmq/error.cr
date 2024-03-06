require "amq-protocol"

module LavinMQ
  class Error < Exception
    class UnexpectedFrame < Error
      getter frame

      def initialize(@frame : AMQ::Protocol::Frame)
        super("Unexpected frame #{frame.class.name}")
      end
    end

    class PreconditionFailed < Error
    end

    class ExchangeTypeError < Error
    end

    class InvalidDefinitions < Error
    end
  end
end
