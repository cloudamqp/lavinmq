require "./message_store"

module LavinMQ
  # Non-destructive peek at the ready messages of a message store, shared by
  # AMQP queues and MQTT sessions. Includers must have a `@msg_store` and
  # guard it with `@msg_store_lock`.
  module Peekable
    # Yields up to `count` PeekedMessages from logical `offset`, in delivery
    # order (requeued first, then from the read head forward). The store lock
    # is only held while copying one message at a time.
    def peek(offset : Int32, count : Int32, max_body : Int32, &block : PeekedMessage -> Nil) : Nil
      return if count <= 0

      requeued_sps, requeued_count = @msg_store_lock.synchronize do
        @msg_store.peek_requeued(offset, count)
      end

      yielded = 0
      requeued_sps.each do |sp|
        if message = peek_copy(sp, redelivered: true, max_body: max_body)
          block.call(message)
          yielded += 1
        end
      end

      peek_segments(Math.max(0, offset - requeued_count), count, yielded, max_body, block)
    rescue MessageStore::ClosedError
      # queue was deleted/closed mid peek, end the result list early
    end

    private def peek_copy(sp : SegmentPosition, redelivered : Bool, max_body : Int32) : PeekedMessage?
      message = @msg_store_lock.synchronize do
        PeekedMessage.new(@msg_store[sp], max_body, redelivered: redelivered) rescue nil
      end
      # without this the peek fiber can reacquire the lock before waiters run
      Fiber.yield
      message
    end

    private def peek_segments(skip : Int32, count : Int32, yielded : Int32, max_body : Int32,
                              block : Proc(PeekedMessage, Nil)) : Nil
      step = nil.as(MessageStore::PeekStep?)
      while yielded < count
        step = @msg_store_lock.synchronize { @msg_store.peek_step(step, skip, max_body) }
        Fiber.yield
        skip -= step.skipped
        if message = step.message
          block.call(message)
          yielded += 1
        end
        break if step.done
      end
    end
  end
end
