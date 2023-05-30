require "./durable_queue"

module LavinMQ
  class StreamQueue < Queue
    private def init_msg_store(data_dir)
      StreamQueueMessageStore.new(data_dir)
    end

    class StreamQueueMessageStore < MessageStore
      def initialize(@data_dir : String)
        super
        # message id? segment position?
      end
    end

    #save message id / segment position
    def publish(msg : Message) : Bool
      return false if @state.closed?
      reject_on_overflow(msg)
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count += 1
      end
      drop_overflow unless immediate_delivery?
      true
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # If nil is returned it means that the delivery limit is reached
    def consume_get(offset, & : Envelope -> Nil) : Bool
      get(offset) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(offset, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      loop do # retry if msg expired or deliver limit hit
        env = @msg_store_lock.synchronize { @msg_store.shift? } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end
        if @delivery_limit && !no_ack
          env = with_delivery_count_header(env) || next
        end
        sp = env.segment_position
        begin
          yield env # deliver the message
        rescue ex   # requeue failed delivery
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          raise ex
        end
        return true
      end
      false
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end
  end

  class DurablStreamQueue < StreamQueue
    @durable = true
  end
end
