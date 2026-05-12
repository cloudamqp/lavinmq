require "sync/shared"
require "./client/client"

module LavinMQ
  class DirectReplyConsumerStore
    @consumers : Sync::Shared(Hash(String, Client::Channel))

    def initialize
      @consumers = Sync::Shared.new(Hash(String, Client::Channel).new, :unchecked)
    end

    def []?(consumer_tag : String) : Client::Channel?
      @consumers.shared { |c| c[consumer_tag]? }
    end

    def []=(consumer_tag : String, channel : Client::Channel) : Client::Channel
      @consumers.lock { |c| c[consumer_tag] = channel }
    end

    def delete(consumer_tag : String) : Client::Channel?
      @consumers.lock(&.delete(consumer_tag))
    end

    def has_key?(consumer_tag : String) : Bool
      @consumers.shared(&.has_key?(consumer_tag))
    end
  end
end
