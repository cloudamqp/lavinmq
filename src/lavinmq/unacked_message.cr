module LavinMQ
  struct UnackedMessage
    include SortableJSON
    getter message_tag : UInt64
    getter consumer_tag : String
    getter delivered_at : String
    getter channel_name : String

    def initialize(message_tag, consumer_tag, delivered_at, channel_name)
      @message_tag = message_tag
      @consumer_tag = consumer_tag
      @delivered_at = delivered_at
      @channel_name = channel_name
    end

    def details_tuple
      {
        message_tag:  @message_tag,
        consumer_tag: @consumer_tag,
        delivered_at: @delivered_at,
        channel_name: @channel_name,
      }
    end
  end
end
