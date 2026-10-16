require "../../segment_position"
require "../../rough_time"

module LavinMQ
  module AMQP
    # A consumer's read position in a Stream, owned and moved by the
    # StreamMessageStore. Included by every kind of stream consumer, through
    # StreamConsumption, so the Stream and its store can serve both AMQP
    # clients and in-process shovels and federation links. An includer also
    # provides:
    #
    # - `tag : String`
    # - `filter_match?(headers) : Bool`
    # - `waiting_for_messages? : Bool` and `notify_new_message`: woken by the
    #   stream on publish
    module StreamCursor
      property offset : Int64 = 0_i64
      property segment : UInt32 = 0_u32
      property pos : UInt32 = 0_u32
      property? segment_acquired = false
      property segment_since = RoughTime.instant # when it moved into its segment
      # Rejected messages to redeliver
      getter requeued = Deque(SegmentPosition).new
    end
  end
end
