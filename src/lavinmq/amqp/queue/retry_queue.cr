require "./queue"
require "./delayed_exchange_queue/delayed_message_store"

module LavinMQ::AMQP
  # Internal queue that parks messages rejected from its primary queue until
  # their backoff delay expires, then publishes them back for redelivery.
  # Backed by DelayedMessageStore so messages expire in delivery-time order.
  # Created and deleted together with the primary queue.
  class RetryQueue < Queue
    getter? internal = true

    @primary_queue : Queue

    MAX_NAME_LENGTH = 256

    def self.create(vhost : VHost, primary_queue : Queue)
      q_name = "amq.retry-#{primary_queue.name}"
      raise LavinMQ::Error::PreconditionFailed.new("Retry queue name too long") if q_name.bytesize > MAX_NAME_LENGTH
      if primary_queue.durable?
        DurableRetryQueue.new(vhost, q_name, primary_queue)
      else
        RetryQueue.new(vhost, q_name, primary_queue)
      end
    end

    protected def initialize(@vhost : VHost, @name : String, @primary_queue : Queue)
      super(@vhost, @name, false, false, AMQP::Table.new)
    end

    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.replicator : nil
      DelayedExchangeQueue::DelayedMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
    end

    def delay(msg : Message) : Bool
      return false if @closed
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.try_send? nil
      ensure_expire_fiber # restart the expire fiber if it died on a transient store error
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Simplified expire loop: no consumers and no per-message TTL to consider,
    # only the backoff delays ordered by the delayed store.
    private def message_expire_loop
      loop do
        if ttl = time_to_message_expiration
          if ttl <= Time::Span::ZERO
            expire_messages
            next
          end
          select
          when @msg_store.empty.when_true.receive
          when @message_ttl_change.receive
          when timeout ttl
            expire_messages
          end
        else
          select
          when @message_ttl_change.receive
          when @msg_store.empty.when_false.receive
            Fiber.yield
          end
        end
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    rescue ::Channel::ClosedError
    ensure
      @message_expire_fiber_active.set(false, :release)
      ensure_expire_fiber # restart if a message arrived during teardown
      @log.debug { "message_expire_loop stopped" }
    end

    # The expire fiber must always run, otherwise parked messages are never redelivered
    private def should_start_expire_fiber? : Bool
      true
    end

    def expire_messages
      @msg_store_lock.synchronize do
        loop do
          env = delayed_msg_store.first_delayed? || break
          if has_expired?(env)
            env = delayed_msg_store.shift_delayed? || break
            expire_msg(env, :expired)
          else
            break
          end
        end
      end
    end

    private def has_expired?(env : Envelope) : Bool
      delay = env.segment_position.delay
      timestamp = env.message.timestamp
      expire_at = timestamp + delay
      expire_at <= RoughTime.unix_ms
    end

    private def delayed_msg_store
      @msg_store.as(DelayedExchangeQueue::DelayedMessageStore)
    end

    private def time_to_message_expiration : Time::Span?
      delayed_msg_store.time_to_next_expiration?
    end

    # Publishes an expired message back to the primary queue, restoring the
    # original timestamp so x-message-ttl keeps applying to the message's total age
    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Retry expired #{sp}, publishing back to #{@primary_queue.name}" }
      timestamp = msg.timestamp
      if headers = msg.properties.headers
        headers.delete("x-delay")
        if original_ts = headers.delete("x-original-timestamp").try(&.as?(Int))
          timestamp = original_ts.to_i64
        end
        msg.properties.headers = headers
      end
      result = @primary_queue.publish(Message.new(timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body)))
      unless result.ok?
        @log.warn { "Dropping retried message #{sp}: primary queue #{@primary_queue.name} returned #{result}" }
      end
      delete_message sp
    end

    # Policies never apply; behavior is governed solely by the
    # primary queue's retry arguments
    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    # Internal queues never auto-expire (x-expires does not apply)
    private def queue_expire_loop
    end

    def publish(message : Message) : PublishResult
      PublishResult::Dropped
    end

    protected def publish_internal(message : Message, dlx_tasks : Argument::DeadLettering::Tasks?) : PublishResult
      PublishResult::Dropped
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      false
    end

    def ack(sp : SegmentPosition) : Nil
    end

    def reject(sp : SegmentPosition, requeue : Bool)
    end

    def requeue(sp : SegmentPosition)
    end
  end

  class DurableRetryQueue < RetryQueue
    def durable?
      true
    end
  end
end
