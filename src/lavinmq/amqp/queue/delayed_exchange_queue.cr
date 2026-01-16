require "./queue"
require "./delayed_exchange_queue/delayed_message_store"

module LavinMQ::AMQP
  # This class is only used by delayed exchanges. It can't niehter should be
  # consumed from or published to by clients.
  class DelayedExchangeQueue < Queue
    MAX_NAME_LENGTH = 256

    @internal = true
    @exchange_name : String

    def self.create(vhost : VHost, exchange_name : String, durable : Bool, auto_delete : Bool)
      q_name = "amq.delayed-#{exchange_name}"
      raise "Exchange name too long" if q_name.bytesize > MAX_NAME_LENGTH

      legacy_q_name = "amq.delayed.#{exchange_name}"
      if use_legacy_name?(vhost.data_dir, legacy_q_name)
        q_name = legacy_q_name
      end

      arguments = AMQP::Table.new({
        "x-dead-letter-exchange" => exchange_name,
        "auto-delete"            => auto_delete,
      })
      if durable
        DurableDelayedExchangeQueue.new(vhost, q_name, false, false, arguments)
      else
        DelayedExchangeQueue.new(vhost, q_name, false, false, arguments)
      end
    end

    private def self.use_legacy_name?(vhost_data_dir, legacy_q_name)
      q_dir_name = Digest::SHA1.hexdigest(legacy_q_name)
      Dir.exists?(Path[vhost_data_dir] / q_dir_name)
    end

    protected def initialize(*args)
      super(*args)
      @exchange_name = arguments["x-dead-letter-exchange"]?.try(&.to_s) || raise "Missing x-dead-letter-exchange"
    end

    def delay(msg : Message) : Bool
      return false if @deleted || @state.closed?
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.send nil
      true
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Overload to use our own store
    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.@replicator : nil
      DelayedMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
    end

    # simplify the message expire loop, as this queue can't have consumers or message-ttl
    private def message_expire_loop
      loop do
        if ttl = time_to_message_expiration
          if ttl <= Time::Span::ZERO
            expire_messages
            next
          end
          select
          when @msg_store.empty.when_true.receive # purge?
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
      @msg_store.as(DelayedMessageStore)
    end

    private def time_to_message_expiration : Time::Span?
      delayed_msg_store.time_to_next_expiration?
    end

    # Overload to not ruin DLX header
    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Expiring #{sp} now due to #{reason}" }
      if headers = msg.properties.headers
        headers.delete("x-delay")
        msg.properties.headers = headers
      end
      @vhost.exchanges[@exchange_name].route_msg Message.new(msg.timestamp, @exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body))
      delete_message sp
    end

    # Disable a lot of inherited functionality (ugly)

    # We don't support any policies
    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      false
    end

    # internal queues can't expire so make this noop
    private def queue_expire_loop
    end

    def publish(message : Message) : Bool
      # This queue should never be published too
      false
    end

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      # noop, not supported
      false
    end

    def ack(sp : SegmentPosition) : Nil
      # noop, not supported
    end

    def reject(sp : SegmentPosition) : Nil
      # noop, not supported
    end
  end

  class DurableDelayedExchangeQueue < DelayedExchangeQueue
    def durable?
      true
    end
  end
end
