require "./durable_queue"
require "../stream_consumer"
require "./stream_queue_message_store"

module LavinMQ::AMQP
  class StreamQueue < DurableQueue
    def initialize(@vhost : VHost, @name : String,
                   @exclusive = false, @auto_delete = false,
                   @arguments = AMQP::Table.new)
      super
      spawn unmap_and_remove_segments_loop, name: "StreamQueue#unmap_and_remove_segments_loop"
    end

    def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?)
      super
      if max_age_value = Policy.merge_definitions(policy, operator_policy)["max-age"]?
        if max_age_policy = parse_max_age(max_age_value.as_s?)
          if current_max = stream_queue_msg_store.max_age
            if current_max > max_age_policy
              stream_queue_msg_store.max_age = max_age_policy
            end
          else
            stream_queue_msg_store.max_age = max_age_policy
          end
        end
      end
      stream_queue_msg_store.max_length = @max_length
      stream_queue_msg_store.max_length_bytes = @max_length_bytes
      stream_queue_msg_store.drop_overflow
    end

    delegate last_offset, new_messages, find_offset, to: @msg_store.as(StreamQueueMessageStore)

    private def message_expire_loop
      # StreamQueues doesn't handle message expiration
    end

    private def queue_expire_loop
      # StreamQueues doesn't handle queue expiration
    end

    private def init_msg_store(data_dir)
      replicator = @vhost.@replicator
      @msg_store = StreamQueueMessageStore.new(data_dir, replicator, metadata: @metadata)
    end

    private def stream_queue_msg_store : StreamQueueMessageStore
      @msg_store.as(StreamQueueMessageStore)
    end

    # save message id / segment position
    def publish(msg : Message) : Bool
      return false if @state.closed?
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count.add(1, :relaxed)
      end
      # Notify all waiting stream consumers about new messages
      notify_all_stream_consumers
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Stream queues does not support basic_get, so always returns `false`
    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      false
    end

    def consume_get(consumer : AMQP::StreamConsumer, & : Envelope -> Nil) : Bool
      get(consumer) do |env|
        yield env
        if env.redelivered
          @redeliver_count.add(1, :relaxed)
        else
          @deliver_count.add(1, :relaxed)
          @deliver_get_count.add(1, :relaxed)
        end
      end
    end

    def store_consumer_offset(consumer_tag : String, offset : Int64) : Nil
      stream_queue_msg_store.store_consumer_offset(consumer_tag, offset)
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(consumer : AMQP::StreamConsumer, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      env = @msg_store_lock.synchronize { @msg_store.shift?(consumer) } || return false
      yield env # deliver the message
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def ack(sp : SegmentPosition) : Nil
    end

    def reject(sp : SegmentPosition, requeue : Bool)
    end

    private def drop_overflow : Nil
      # Overflow handling is done in StreamQueueMessageStore
    end

    private def notify_all_stream_consumers
      @consumers.each do |consumer|
        if stream_consumer = consumer.as?(AMQP::StreamConsumer)
          stream_consumer.notify_new_message if stream_consumer.waiting_for_messages?
        end
      end
    end

    private def handle_arguments
      super
      @effective_args << "x-queue-type"
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
      stream_queue_msg_store.max_age = parse_max_age(@arguments["x-max-age"]?)
      stream_queue_msg_store.max_length = @max_length
      stream_queue_msg_store.max_length_bytes = @max_length_bytes
      stream_queue_msg_store.drop_overflow
    end

    private def parse_max_age(value) : Time::Span | Time::MonthSpan | Nil
      return if value.nil?
      if str = value.as?(String)
        if match = str.match(/\A(\d+)([YMDhms])\z/)
          int = match[1].to_i64
          case match[2]
          when "s" then Time::Span.new(seconds: int)
          when "m" then Time::Span.new(minutes: int)
          when "h" then Time::Span.new(hours: int)
          when "D" then Time::Span.new(days: int)
          when "M" then Time::MonthSpan.new(int)
          when "Y" then Time::MonthSpan.new(int * 12)
          else          raise LavinMQ::Error::PreconditionFailed.new("max-age unit unit")
          end
        else
          raise LavinMQ::Error::PreconditionFailed.new("max-age format invalid")
        end
      else
        raise LavinMQ::Error::PreconditionFailed.new("max-age must be a string")
      end
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      delete_count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
      @log.info { "Purged #{delete_count} messages" }
      delete_count
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    private def unmap_and_remove_segments_loop
      sleep rand(60).seconds
      until closed?
        sleep 60.seconds
        unmap_and_remove_segments
      end
    end

    private def unmap_and_remove_segments
      used_segments = Set(UInt32).new
      @consumers_lock.synchronize do
        @consumers.each do |consumer|
          used_segments << consumer.as(AMQP::StreamConsumer).segment
        end
      end
      @msg_store_lock.synchronize do
        stream_queue_msg_store.drop_overflow
        stream_queue_msg_store.unmap_segments(except: used_segments)
      end
    end
  end
end
