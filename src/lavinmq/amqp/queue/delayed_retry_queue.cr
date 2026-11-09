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

    # Its primary queue recreates it once it's closed, see
    # Queue#active_retry_queue, so a restart goes through it: reopening this
    # instance as well would load a second store from the same files
    def restart! : Bool
      return false unless @closed
      !@primary_queue.recreate_retry_queue.nil?
    end

    # A recreated retry queue is registered in the definitions after the one
    # it replaced (see Queue#active_retry_queue), so a delete by name in
    # between finds this closed one. The new one uses the same files, so the
    # delete goes to it, instead of removing its files from under it. For the
    # same reason the primary queue doesn't recreate this one while its
    # delete waits for its copies' drain, see Queue#start_retry_queue_delete.
    def delete : Bool
      if current = @primary_queue.start_retry_queue_delete(self)
        return current.delete
      end
      begin
        super
      ensure
        @primary_queue.retry_queue_deleted(self)
      end
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
        return @primary_queue.dead_letter_delayed(msg, :expired) { |copied, stored| delete_message sp, copied: copied if stored }
      end
      @log.debug { "Retry expired #{sp}, publishing back to #{@primary_queue.name}" }
      result = @primary_queue.publish_internal(Message.moved_copy(msg))
      return redelay(env) unless result.ok?
      delete_message sp, copied: true
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
      if delay(Message.moved_copy(msg))
        @log.info { "Primary queue #{@primary_queue.name} unavailable, delaying message #{sp} for another #{backoff}ms" }
        delete_message sp, copied: true
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
