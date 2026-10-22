require "./delayed_queue"

module LavinMQ::AMQP
  # Internal queue that holds messages rejected from its primary queue until
  # their backoff delay expires, then publishes them back for redelivery.
  # Created and deleted together with the primary queue.
  class DelayedRetryQueue < DelayedQueue
    @primary_queue : Queue
    getter? draining = false

    def self.queue_name(primary_queue_name : String) : String
      "amq.retry-#{primary_queue_name}"
    end

    def self.create(vhost : VHost, primary_queue : Queue)
      q_name = queue_name(primary_queue.name)
      raise LavinMQ::Error::PreconditionFailed.new("Retry queue name too long") if q_name.bytesize > MAX_NAME_LENGTH
      if primary_queue.durable?
        DurableDelayedRetryQueue.new(vhost, q_name, primary_queue)
      else
        DelayedRetryQueue.new(vhost, q_name, primary_queue)
      end
    end

    protected def initialize(@vhost : VHost, @name : String, @primary_queue : Queue)
      super(@vhost, @name, false, false, AMQP::Table.new)
    end

    # Set under @msg_store_lock, which #push_delayed holds across its check and
    # push, so no delay can land after the drained check finds the queue empty
    def drain : Nil
      return if @draining
      @msg_store_lock.synchronize { @draining = true }
      @log.info { "Retries disabled, draining delayed messages back to #{@primary_queue.name}" }
      notify_if_drained
    end

    def resume_retries : Nil
      @draining = false
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      super.tap { notify_if_drained }
    end

    private def notify_if_drained : Nil
      return if !@draining || @closed || !empty?
      spawn(name: "DelayedRetryQueue#drained #{@vhost.name}/#{@name}") do
        @primary_queue.retry_queue_drained(self)
      end
    end

    def primary_queue_name : String?
      @primary_queue.name
    end

    # Refused while draining: a retry disabled by policy must not delay new
    # messages, and the drained queue is about to be deleted
    def delay(msg : Message) : Bool
      push_delayed(msg, refuse_while_draining: true)
    end

    private def push_delayed(msg : Message, *, refuse_while_draining : Bool) : Bool
      @msg_store_lock.synchronize do
        return false if @closed || (refuse_while_draining && @draining)
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.try_send? nil
      ensure_expire_fiber # restart the expire fiber if it died on a transient store error
      true
    rescue MessageStore::ClosedError
      false
    rescue ex : MessageStore::Error | IO::Error
      @log.error(ex) { "Queue closed due to error, message requeued instantly" }
      close
      false
    end

    # Publishes an expired message back to the primary queue. The message kept
    # its original timestamp while delayed, so x-message-ttl keeps applying to
    # the message's total age; a message that outlived it is dead-lettered by
    # the primary queue instead of being redelivered
    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      delivery_count = nil
      if headers = msg.properties.headers
        headers.delete("x-delay")
        delivery_count = headers["x-delivery-count"]?.try(&.as?(Int)).try(&.to_i32)
        headers.delete("x-delivery-count") unless @primary_queue.counts_deliveries?
        msg.properties.headers = headers
      end
      if @primary_queue.expired_while_delayed?(msg)
        @log.debug { "Retry #{sp} expired by message TTL, dead-lettering from #{@primary_queue.name}" }
        @primary_queue.dead_letter_delayed(msg, :expired) { delete_message sp }
        return notify_if_drained
      end
      @log.debug { "Retry expired #{sp}, publishing back to #{@primary_queue.name}" }
      result = @primary_queue.publish_internal(Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body)), nil, delivery_count: delivery_count)
      return redelay(env) unless result.ok?
      delete_message sp
      notify_if_drained
    end

    # The primary queue is full with overflow=reject-publish, or closed; delay
    # the message again for one more backoff period instead of losing it
    private def redelay(env : Envelope) : Nil
      sp = env.segment_position
      msg = env.message
      h = msg.properties.headers || AMQP::Table.new
      delivery_count = h["x-delivery-count"]?.try(&.as?(Int)).try(&.to_i32) || 1
      backoff = @primary_queue.retry_delay_for(delivery_count)
      h["x-delay"] = @primary_queue.delay_from(msg, backoff)
      msg.properties.headers = h
      redelayed = Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body))
      if push_delayed(redelayed, refuse_while_draining: false)
        @log.info { "Primary queue #{@primary_queue.name} unavailable, delaying message #{sp} for another #{backoff}ms" }
        delete_message sp
      else
        @log.warn { "Retry queue closed, message #{sp} kept on disk until restart" }
      end
    end
  end

  class DurableDelayedRetryQueue < DelayedRetryQueue
    def durable?
      true
    end
  end
end
