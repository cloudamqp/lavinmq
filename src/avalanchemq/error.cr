require "amq-protocol"

module AvalancheMQ
  class Error < Exception
    class UnexpectedFrame < Error
      getter channel : UInt16
      getter class_id : UInt16
      getter method_id : UInt16

      def initialize(frame : AMQ::Protocol::Frame::Method)
        @channel = frame.channel
        @class_id = frame.class_id
        @method_id = frame.method_id
        super("Unexpected frame #{frame.class.name}")
      end

      def initialize(frame : AMQ::Protocol::Frame)
        @channel = frame.channel
        @class_id = 0_u16
        @method_id = 0_u16
        super("Unexpected frame #{frame.class.name}")
      end
    end
    class PreconditionFailed < Error
    end
  end
end
