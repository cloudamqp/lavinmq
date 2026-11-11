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

    # A retry queue never closes on its own, only with its primary queue, so
    # a reject never closes or deletes it, which would take the definitions
    # lock while the rejecting channel holds its own lock.
    # The loop is restarted by the next delay.
    private def expire_loop_failed(ex : MessageStore::Error) : Nil
      @log.error(ex) { "Release of delayed messages stopped due to error, restarted by the next delay" }
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
      # Not closed, like other queues on a store error: the message is
      # requeued instantly, and the next reject tries the store again
      @log.error(ex) { "Failed to delay message, requeued instantly" }
      false
    end

    # Unlike other internal queues, data that can't be read pauses the retry
    # queue, so that it isn't given up before someone has decided, as for its
    # primary queue: while paused nothing is released, and rejects are still
    # delayed
    private def pauses_for_unreadable_data? : Bool
      true
    end

    # Publishes an expired message back to the primary queue. The message kept
    # its original timestamp while delayed, so x-message-ttl keeps applying to
    # the message's total age; a message that outlived it is dead-lettered by
    # the primary queue instead of being redelivered
    private def expire_msg(env : Envelope, reason : Symbol)
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
      elsif @closed
        @log.warn { "Retry queue closed, message #{sp} kept on disk until restart" }
      else
        # The store couldn't take the copy, so the message itself is delayed
        # again, rather than left out of the index
        @msg_store_lock.synchronize { delayed_msg_store.delay_again(sp, backoff) }
        @log.warn { "Primary queue #{@primary_queue.name} unavailable, retrying message #{sp} in #{backoff}ms" }
      end
    end
  end

  class DurableDelayedRetryQueue < DelayedRetryQueue
    def durable?
      true
    end
  end
end
