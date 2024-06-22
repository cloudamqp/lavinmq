module LavinMQ
  class UnackedMessage
    include SortableJSON
    getter message_tag : UInt64
    getter consumer_tag : String
    getter delivered_at : String

    def initialize(message_tag, consumer_tag, delivered_at)
      @message_tag = message_tag
      @consumer_tag = consumer_tag
      @delivered_at = delivered_at
    end

    def details_tuple
      {
        message_tag:  @message_tag,
        consumer_tag: @consumer_tag,
        delivered_at: @delivered_at,
      }
    end
  end
end
