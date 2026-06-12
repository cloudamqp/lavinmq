require "./client/client"

module LavinMQ
  class DirectReplyConsumerStore
    def initialize
      @consumers = Hash(String, Client::Channel).new
    end

    def []?(consumer_tag : String) : Client::Channel?
      @consumers[consumer_tag]?
    end

    def []=(consumer_tag : String, channel : Client::Channel) : Client::Channel
      @consumers[consumer_tag] = channel
    end

    def delete(consumer_tag : String) : Client::Channel?
      @consumers.delete(consumer_tag)
    end

    def has_key?(consumer_tag : String) : Bool
      @consumers.has_key?(consumer_tag)
    end
  end
end
