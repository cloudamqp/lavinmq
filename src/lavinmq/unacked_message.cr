module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter message_tag : UInt64
    getter consumer_tag : String
    getter unacked_for_seconds : Float64
    getter channel_name : String

    def initialize(message_tag, consumer_tag, unacked_for_seconds, channel_name)
      @message_tag = message_tag
      @consumer_tag = consumer_tag
      @unacked_for_seconds = unacked_for_seconds
      @channel_name = channel_name
    end

    def details_tuple
      {
        message_tag:  @message_tag,
        consumer_tag: @consumer_tag,
        unacked_for_seconds: @unacked_for_seconds,
        channel_name: @channel_name,
      }
    end
  end
end
