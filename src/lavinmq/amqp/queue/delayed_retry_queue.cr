require "./delayed_queue"

module LavinMQ::AMQP
  # Internal queue that holds messages rejected from its primary queue until
  # their backoff delay expires, then publishes them back for redelivery.
  # Created and deleted together with the primary queue.
  class DelayedRetryQueue < DelayedQueue
    @primary_queue : Queue

    def self.create(vhost : VHost, primary_queue : Queue)
      q_name = "amq.retry-#{primary_queue.name}"
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

    # Only the primary queue's restart replaces a retry queue
    def restart! : Bool
      false
    end

    # A retry queue never closes on its own, only with its primary queue, so
    # a reject never closes or deletes it, which would take the definitions
    # lock while the rejecting channel holds its own lock.
    # The loop is restarted by the next delay.
    private def expire_loop_failed(ex : MessageStore::Error) : Nil
      @log.error(ex) { "Release of delayed messages stopped due to error, restarted by the next delay" }
    end

    private def purge_failed(ex : MessageStore::Error) : Nil
      @log.error(ex) { "Purge failed due to error" }
    end

    def delay(msg : Message) : Bool
      return false if @closed
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
      end
      @publish_count.add(1, :relaxed)
      @message_ttl_change.try_send? nil
      ensure_expire_fiber # restart the expire fiber if it stopped on a store error
      true
    rescue MessageStore::ClosedError
      false
    rescue ex : MessageStore::Error | IO::Error
      @log.error(ex) { "Failed to delay message, requeuing instantly" }
      false
    end

    # Publishes an expired message back to the primary queue. The message kept
    # its original timestamp while delayed, so x-message-ttl keeps applying to
    # the message's total age; a message that outlived it is dead-lettered by
    # the primary queue instead of being redelivered
    private def expire_msg(env : Envelope, reason : Symbol)
      # Kept on disk for the retry queue that replaces this one when it's
      # recreated or restarted, see Queue#delete_message
      return if @closed
      sp = env.segment_position
      msg = env.message
      if headers = msg.properties.headers
        headers.delete("x-delay")
        msg.properties.headers = headers
      end
      if @primary_queue.expired_while_delayed?(msg)
        @log.debug { "Retry #{sp} expired by message TTL, dead-lettering from #{@primary_queue.name}" }
        return @primary_queue.dead_letter_delayed(msg, :expired) { |stored| delete_message sp if stored }
      end
      @log.debug { "Retry expired #{sp}, publishing back to #{@primary_queue.name}" }
      result = @primary_queue.publish_internal(Message.moved_copy(msg))
      return redelay(env) unless result.ok?
      delete_message sp
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
      redelayed = Message.moved_copy(msg)
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
