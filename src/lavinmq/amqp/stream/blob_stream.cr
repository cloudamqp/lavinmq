require "./stream"
require "./blob_message_store"

module LavinMQ::AMQP
  class BlobStream < Stream
    private def init_msg_store(data_dir)
      replicator = @vhost.@replicator
      @msg_store = BlobMessageStore.new(data_dir, replicator, true, metadata: @metadata)
    end

    def add_consumer(consumer : Client::Channel::Consumer)
      super
      blob_msg_store.add_consumer(consumer.tag, consumer.as(AMQP::StreamConsumer).segment)
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      super
      blob_msg_store.remove_consumer(consumer.tag)
    end

    private def blob_msg_store : BlobMessageStore
      @msg_store.as(BlobMessageStore)
    end
  end
end
