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

    def drain : Nil
      return if @draining
      @draining = true
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

    def delay(msg : Message) : Bool
      return false if @closed
      @msg_store_lock.synchronize do
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
    # the message's total age
    private def expire_msg(env : Envelope, reason : Symbol)
      sp = env.segment_position
      msg = env.message
      @log.debug { "Retry expired #{sp}, publishing back to #{@primary_queue.name}" }
      if headers = msg.properties.headers
        headers.delete("x-delay")
        msg.properties.headers = headers
      end
      result = @primary_queue.publish_internal(Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body)))
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
      h["x-delay"] = @primary_queue.delay_from(msg.timestamp, backoff)
      msg.properties.headers = h
      redelayed = Message.new(msg.timestamp, msg.exchange_name, msg.routing_key,
        msg.properties, msg.bodysize, IO::Memory.new(msg.body))
      if delay(redelayed)
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
