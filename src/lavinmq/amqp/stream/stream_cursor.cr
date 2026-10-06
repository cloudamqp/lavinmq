module LavinMQ
  module AMQP
    # A consumer's read position in a Stream. Included by every kind of stream
    # consumer, AMQP::StreamConsumer for AMQP clients and
    # Endpoint::LocalStreamConsumer for in-process shovels and federation
    # links, so the Stream and its store can serve both. An includer provides:
    #
    # - `tag : String`
    # - `offset`, `segment`, `pos`, `segment_since` and `segment_acquired?`,
    #   with setters: the read position, owned by StreamMessageStore
    # - `requeued : Deque(SegmentPosition)`: rejected messages to redeliver
    # - `filter_match?(headers) : Bool`
    # - `waiting_for_messages? : Bool` and `notify_new_message`: woken by the
    #   stream on publish
    module StreamCursor
    end
  end
end
