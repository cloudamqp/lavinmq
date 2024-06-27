module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter message_tag, consumer_tag, channel, delivered_at

    def initialize(@channel : LavinMQ::Client::Channel, @message_tag : UInt64, @delivered_at : Time::Span, @consumer_tag : String? = nil)
    end

    def details_tuple
      {
        message_tag:         @message_tag,
        consumer_tag:        @consumer_tag || "",
        unacked_for_seconds: (RoughTime.monotonic - delivered_at).to_i,
        channel_name:        @channel.name,
      }
    end
  end
end
