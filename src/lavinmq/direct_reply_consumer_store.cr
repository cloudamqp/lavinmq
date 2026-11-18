require "./client/client"

module LavinMQ
  class DirectReplyConsumerStore
    def initialize
      @consumers = Hash(String, AMQP::Channel).new
    end

    def []?(consumer_tag : String) : AMQP::Channel?
      @consumers[consumer_tag]?
    end

    def []=(consumer_tag : String, channel : AMQP::Channel) : AMQP::Channel
      @consumers[consumer_tag] = channel
    end

    def delete(consumer_tag : String) : AMQP::Channel?
      @consumers.delete(consumer_tag)
    end

    def has_key?(consumer_tag : String) : Bool
      @consumers.has_key?(consumer_tag)
    end
  end
end
