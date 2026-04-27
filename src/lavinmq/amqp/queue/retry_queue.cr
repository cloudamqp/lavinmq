require "./queue"
require "./delayed_exchange_queue/delayed_message_store"

module LavinMQ::AMQP
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
      replicator = durable? ? @vhost.@replicator : nil
      DelayedExchangeQueue::DelayedMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
    end

    def delay(msg : Message) : Bool
      return false if @deleted || @state.closed?
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.try_send? nil
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

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
    rescue ::Channel::ClosedError
    ensure
      @log.debug { "message_expire_loop stopped" }
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
      @primary_queue.publish(Message.new(timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body)))
      delete_message sp
    end

    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    private def queue_expire_loop
    end

    def publish(message : Message) : Bool
      false
    end

    protected def publish_internal(message : Message, dlx_tasks : Argument::DeadLettering::Tasks?) : Bool
      false
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      false
    end

    def ack(sp : SegmentPosition) : Nil
    end

    def reject(sp : SegmentPosition, requeue : Bool)
    end
  end

  class DurableRetryQueue < RetryQueue
    def durable?
      true
    end
  end
end
