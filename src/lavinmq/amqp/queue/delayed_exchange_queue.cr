require "./queue"

module LavinMQ::AMQP
  class DelayedExchangeQueue < Queue
    @internal = true

    @exchange_name : String

    def initialize(*args)
      super(*args)
      @exchange_name = arguments["x-dead-letter-exchange"]?.try(&.to_s) || raise "Missing x-dead-letter-exchange"
    end

    private def init_msg_store(data_dir)
      replicator = durable? ? @vhost.@replicator : nil
      DelayedMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
    end

    private def expire_at(msg : BytesMessage) : Int64?
      msg.timestamp + (msg.delay || 0u32)
    end

    # internal queues can't expire so make this noop
    private def queue_expire_loop
    end

    # simplify the message expire loop, as this queue can't have consumers or message-ttl
    private def message_expire_loop
      loop do
        if ttl = time_to_message_expiration
          select
          when @msg_store.empty.when_true.receive # there's a new "first" message
          when timeout ttl
            expire_messages
          end
        else
          @msg_store.empty.when_false.receive
        end
      end
    rescue ::Channel::ClosedError
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
      if ex = @vhost.fetch_exchange(@exchange_name)
        ex.route_msg Message.new(msg.timestamp, @exchange_name, msg.routing_key, msg.properties, msg.bodysize, IO::Memory.new(msg.body))
      else
        @log.error { "Exchange #{@exchange_name} not found, can't route expired message #{sp}" }
      end
      delete_message sp
    end

    class DelayedMessageStore < MessageStore
      def initialize(*args, **kwargs)
        super
        order_messages
      end

      def order_messages
        sps = Array(SegmentPosition).new(@size)
        while env = shift?
          sps << env.segment_position
        end
        sps.each { |sp| requeue sp }
      end

      def push(msg) : SegmentPosition
        sp = super
        # make sure that we don't read from disk, only from requeued
        @rfile_id = @wfile_id
        @rfile = @wfile
        @rfile.seek(0, IO::Seek::End)
        # order messages by priority in the requeue dequeue
        idx = @requeued.bsearch_index do |rsp|
          if rsp.delay == sp.delay
            rsp > sp
          else
            rsp.delay > sp.delay
          end
        end
        if idx
          @requeued.insert(idx, sp)
          if idx.zero?
            @empty.set false
          end
        else
          @requeued.push(sp)
        end
        sp
      end

      def requeue(sp : SegmentPosition)
        idx = @requeued.bsearch_index do |rsp|
          if rsp.delay == sp.delay
            rsp > sp
          else
            rsp.delay > sp.delay
          end
        end
        if idx
          @requeued.insert(idx, sp)
        else
          @requeued.push(sp)
        end
        was_empty = @size.zero?
        @bytesize += sp.bytesize
        @size += 1
        @empty.set false if was_empty
      end
    end
  end

  class DurableDelayedExchangeQueue < DelayedExchangeQueue
    def durable?
      true
    end
  end
end
