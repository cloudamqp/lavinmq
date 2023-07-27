require "./durable_queue"
require "./stream_queue_message_store"

module LavinMQ
  class StreamQueue < Queue
    @durable = true

    def new_messages : Channel(Bool)
      stream_queue_msg_store.new_messages
    end

    def find_offset(consumer : StreamPosition = DefaultPosition::Instance) : Nil
      stream_queue_msg_store.find_offset(consumer)
    end

    private def message_expire_loop
      # StreamQueues doesn't handle message expiration
    end

    private def queue_expire_loop
      # StreamQueues doesn't handle queue expiration
    end

    private def init_msg_store(data_dir)
      replicator = @vhost.@replicator
      @msg_store = StreamQueueMessageStore.new(data_dir, replicator)
    end

    private def stream_queue_msg_store : StreamQueueMessageStore
      @msg_store.as(StreamQueueMessageStore)
    end

    # save message id / segment position
    def publish(msg : Message) : Bool
      return false if @state.closed?
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count += 1
      end
      drop_overflow
      true
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ex
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool # TODO: does basic_get make sense for stream queues?
      # raise NotImplementedError.new("Basic get not implemented for stream queues")
      false
    end

    def consume_get(consumer : StreamPosition, & : Envelope -> Nil) : Bool
      get(consumer) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(consumer : StreamPosition, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      env = @msg_store_lock.synchronize { @msg_store.shift?(consumer) } || return false
      yield env # deliver the message
      true
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def ack(sp : SegmentPosition) : Nil
    end

    def reject(sp : SegmentPosition, requeue : Bool)
    end

    def last_offset : Int64
      stream_queue_msg_store.last_offset
    end

    private def handle_arguments
      super
      if @dlx
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange not allowed for stream queues")
      end
      if @dlrk
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange not allowed for stream queues")
      end
      if @expires
        raise LavinMQ::Error::PreconditionFailed.new("x-expires not allowed for stream queues")
      end
      if @delivery_limit
        raise LavinMQ::Error::PreconditionFailed.new("x-delivery-limit not allowed for stream queues")
      end
      if @reject_on_overflow
        raise LavinMQ::Error::PreconditionFailed.new("x-overflow not allowed for stream queues")
      end
      if @single_active_consumer_queue
        raise LavinMQ::Error::PreconditionFailed.new("x-single-active-consumer not allowed for stream queues")
      end
    end
  end
end
