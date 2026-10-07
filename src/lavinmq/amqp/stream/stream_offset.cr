require "amq-protocol"
require "../../error"

module LavinMQ::AMQP
  # Where a stream reader starts: the parsed `x-stream-offset` consumer
  # argument, or the `offset` of an HTTP stream read.
  module StreamOffset
    record First

    # The first message of the last segment
    record Last

    # After the last message, only new messages are read
    record Next

    record Absolute, value : Int64 do
      def self.new(value : Int)
        new(value.to_i64)
      end
    end

    # The `count`:th message from the end, 1 is the last message
    record FromEnd, count : Int64 do
      def self.new(count : Int)
        new(count.to_i64)
      end
    end

    # The first message published at or after `time`
    record Timestamp, time : Time

    alias Any = First | Last | Next | Absolute | FromEnd | Timestamp

    class Error < Exception
      def initialize(offset)
        super("invalid offset #{offset}")
      end
    end

    # Parses the `x-stream-offset` consumer argument, nil when not given
    def self.from_amqp(value : AMQ::Protocol::Field) : Any?
      case value
      when Nil     then nil
      when "first" then First.new
      when "last"  then Last.new
      when "next"  then Next.new
      when Time    then Timestamp.new(value)
      when Int     then from_int(value)
      else
        raise LavinMQ::Error::PreconditionFailed.new("x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
      end
    end

    # Parses the offset of an HTTP stream read, first when not given
    def self.parse(value : String) : Any
      case value
      when "first"   then First.new
      when "last"    then Last.new
      when "next"    then Next.new
      when /^-?\d+$/ then from_int(value.to_i64? || raise Error.new(value))
      else                raise Error.new(value)
      end
    end

    def self.parse(value : Int) : Any
      from_int(value)
    end

    def self.parse(value : Nil) : Any
      First.new
    end

    def self.parse(value) : Any
      raise Error.new(value)
    end

    # Negative integers count from the end
    private def self.from_int(value : Int) : Any
      return Absolute.new(value) unless value.negative?
      FromEnd.new(value == Int64::MIN ? Int64::MAX : -value.to_i64)
    end
  end
end
