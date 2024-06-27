module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter delivery_tag, consumer_tag, channel, delivered_at

    def initialize(@channel : LavinMQ::Client::Channel, @delivery_tag : UInt64, @delivered_at : Time::Span, @consumer_tag : String? = nil)
    end

    def details_tuple
      {
        delivery_tag:        @delivery_tag,
        consumer_tag:        @consumer_tag || "Basic get",
        unacked_for_seconds: (RoughTime.monotonic - delivered_at).to_i,
        channel_name:        @channel.name,
      }
    end
  end
end
