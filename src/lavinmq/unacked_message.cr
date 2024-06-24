module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter message_tag, consumer_tag, unacked_for_seconds, channel_name

    def initialize(@message_tag : UInt64, @consumer_tag : String, @unacked_for_seconds : Int64, @channel_name : String)
    end

    def details_tuple
      {
        message_tag:         @message_tag,
        consumer_tag:        @consumer_tag,
        unacked_for_seconds: @unacked_for_seconds,
        channel_name:        @channel_name,
      }
    end
  end
end
