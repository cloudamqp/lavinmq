module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter message_tag, consumer_tag, channel_name, delivered_at

    def initialize(@message_tag : UInt64, @consumer_tag : String, @channel_name : String, @delivered_at : Time::Span)
    end

    def details_tuple
      {
        message_tag:         @message_tag,
        consumer_tag:        @consumer_tag,
        unacked_for_seconds: (RoughTime.monotonic - delivered_at).to_i,
        channel_name:        @channel_name,
      }
    end
  end
end
