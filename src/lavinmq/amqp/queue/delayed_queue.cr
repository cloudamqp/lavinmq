require "./queue"
require "./delayed_queue/delayed_message_store"

module LavinMQ::AMQP
  # Base class for the internal queues that hold messages until a per-message
  # delay expires: the delayed exchange queue and the retry queue. Backed by
  # DelayedMessageStore so messages expire in delivery-time order.
  abstract class DelayedQueue < Queue
    MAX_NAME_LENGTH = 255

    getter? internal = true

    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.replicator : nil
      DelayedMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
    end

    # Simplified expire loop: no consumers and no per-message TTL to consider,
    # only the delays ordered by the delayed store.
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

    # The expire fiber must always run, otherwise delayed messages are never released
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
      @msg_store.as(DelayedMessageStore)
    end

    private def time_to_message_expiration : Time::Span?
      delayed_msg_store.time_to_next_expiration?
    end

    # Policies never apply to internal queues
    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    # Internal queues never auto-expire (x-expires does not apply)
    private def queue_expire_loop
    end

    # Client operations are not supported on internal queues

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
end

require "./delayed_exchange_queue"
require "./delayed_retry_queue"
