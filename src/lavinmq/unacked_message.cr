require "./sortable_json"

module LavinMQ
  struct UnackedMessage
    include SortableJSON

    getter delivery_tag, consumer_tag, channel, delivered_at

    def initialize(@channel : Client::Channel, @delivery_tag : UInt64, @delivered_at : Time::Span, @consumer_tag : String? = nil)
    end

    def details_tuple
      {
        delivery_tag:        @delivery_tag,
        consumer_tag:        @consumer_tag || "Basic get",
        unacked_for_seconds: (RoughTime.monotonic - delivered_at).to_i,
        channel_name:        @channel.name,
      }
    end

    def search_match?(value : String) : Bool
      @delivery_tag.to_s == value
    end

    def search_match?(value : Regex) : Bool
      value === @delivery_tag.to_s
    end
  end
end
