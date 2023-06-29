require "./durable_queue"
require "./stream_queue_message_store"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    @durable = true
    @exclusive_consumer = false
    @no_ack = true
    getter new_messages : Channel(Bool)

    def initialize(@vhost : VHost, @name : String,
      @exclusive = false, @auto_delete = false,
      @arguments = Hash(String, AMQP::Field).new)
      super
      @new_messages = @msg_store.as(StreamQueueMessageStore).new_messages
    end

    private def init_msg_store(data_dir)
      replicator = @vhost.@replicator
      @msg_store = StreamQueueMessageStore.new(data_dir, replicator)
    end

    # save message id / segment position
    def publish(msg : Message) : Bool
      return false if @state.closed?
      reject_on_overflow(msg)
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count += 1
      end
      drop_overflow unless immediate_delivery?
      true
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ex
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      return false if !@state.running? && (@state.paused? && !force)
      @last_get_time = RoughTime.monotonic
      @queue_expiration_ttl_change.try_send? nil
      @get_count += 1
      get(true) do |env|
        yield env
      end
    end

    # If nil is returned it means that the delivery limit is reached
    def consume_get(consumer : Client::Channel::StreamConsumer, & : Envelope -> Nil) : Bool
      get(consumer) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(consumer : Client::Channel::StreamConsumer, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      loop do # retry if msg expired or deliver limit hit
        env = @msg_store_lock.synchronize { @msg_store.shift?(consumer) } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end

        sp = env.segment_position
        begin
          yield env # deliver the message
end
        return true
      end
      false
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def ack(sp : SegmentPosition) : Nil
    end

    # do nothing?
    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted || @closed
      true
    end

    def empty?(consumer) : Bool
      @msg_store.as(StreamQueueMessageStore).empty?(consumer)
    end

    def last_offset : Int64
      @msg_store.as(StreamQueueMessageStore).last_offset
    end
  end
end
