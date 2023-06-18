require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    @durable = true
    @exclusive_consumer = false
    @no_ack = true
    @last_offset = 0

    private def init_msg_store(data_dir)
      @msg_store = StreamQueueMessageStore.new(data_dir)
    end

    class StreamQueueMessageStore < MessageStore
      def initialize(@data_dir : String)
        super
      end

      def shift?(consumer) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
        seg = consumer.segment || @segments.first_value
        pos = consumer.pos || 0_u32
        requeued = consumer.requeued || Deque(SegmentPosition).new
        raise ClosedError.new if @closed
        if sp = requeued.shift?           # handle requeued per consumer
          segment = @segments[sp.segment] #
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
            @bytesize -= sp.bytesize
            @size -= 1
            notify_empty(true) if @size.zero?
            return Envelope.new(sp, msg, redelivered: true)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end

        loop do
          seg = if consumer.segment && consumer.segment != 0
                  consumer.segment
                else
                  @rfile_id
                end
          rfile = @segments[seg]
          pos = if consumer.pos && consumer.pos != 0
                  consumer.pos
                else
                  rfile.pos.to_u32
                end

          pos ||= 0_u32
          seg ||= 0_u32

          if pos == rfile.size # EOF?
            select_next_read_segment && next
            return if @size.zero?
            raise IO::EOFError.new("EOF but @size=#{@size}")
          end
          if deleted?(seg, pos)
            BytesMessage.skip(rfile)
            next
          end
          msg = BytesMessage.from_bytes(rfile.to_slice + pos)
          puts "-------------"
          puts msg.inspect
          puts "-----------------"
          sp = SegmentPosition.make(seg, pos, msg)
          rfile.seek(sp.bytesize, IO::Seek::Current)
          @bytesize -= sp.bytesize
          @size -= 1
          notify_empty(true) if @size.zero?
          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(@rfile, cause: ex)
        end
      end

      # should delete without ack
      def delete(sp) : Nil
      end
    end

    def add_offset_header(msg, offset)
      headers = msg.properties.headers || ::AMQP::Client::Arguments.new
      headers["x-stream-offset"] = offset.as(AMQ::Protocol::Field)
      msg.properties.headers = headers
      msg
    end

    # save message id / segment position
    def publish(msg : Message) : Bool
      return false if @state.closed?
      reject_on_overflow(msg)
      @msg_store_lock.synchronize do
        offset = @last_offset += 1
        msg = add_offset_header(msg, offset) # save last_index in RAM and update and set it as offset? #how to set first offset on startup/new queue?
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

    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      return false if !@state.running? && (@state.paused? && !force)
      @last_get_time = RoughTime.monotonic
      @queue_expiration_ttl_change.try_send? nil
      @get_count += 1
      get(true) do |env|
        yield env
      end
    end

    # If nil is returned it means that the delivery limit is reached
    def consume_get(consumer : Client::Channel::StreamConsumer, & : Envelope -> Nil) : Bool
      get(consumer) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(consumer : Client::Channel::StreamConsumer, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      loop do # retry if msg expired or deliver limit hit
        env = @msg_store_lock.synchronize { @msg_store.shift?(consumer) } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end
        offset = consumer.offset || 0_u64
        puts "::::get::::"
        puts "env.message.offset: #{env.message.offset}"
        puts "offset: #{offset}"
        headers = env.message.properties.headers

        msg_offset = 0
        if ht = headers.as?(AMQ::Protocol::Table)
          msg_offset = ht["x-stream-offset"].as(Int32)
          puts "msg_offset: #{msg_offset}"
        end

        # some testing for finding the right message by offset, should be per consumer
        # and we should probably also save segment/position per consumer
        # but we also need a "seek" if the offset is changed or a new consumer is added
        if msg_offset < offset
          puts "env.message.offset #{msg_offset} < offset #{offset}}"
          next
        end
        sp = env.segment_position
        consumer.update_offset(msg_offset.to_u64)
        consumer.update_segment(env.segment_position.segment, env.segment_position.position)
        if consumer.no_ack
          begin
            yield env # deliver the message
          rescue ex   # requeue failed delivery
            @msg_store_lock.synchronize { @msg_store.requeue(sp) }
            raise ex
          end
          delete_message(sp)
        else
          mark_unacked(sp) do
            yield env # deliver the message
          end
        end
        return true
      end
      false
    rescue ex : MessageStore::Error
      @log.error(exception: ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    # handle offset
    def add_consumer(consumer : Client::Channel::StreamConsumer)
      return if @closed
      @last_get_time = RoughTime.monotonic
      @consumers_lock.synchronize do
        was_empty = @consumers.empty?
        @consumers << consumer
        notify_consumers_empty(false) if was_empty
      end
      @exclusive_consumer = true if consumer.exclusive
      @has_priority_consumers = true unless consumer.priority.zero?
      @log.debug { "Adding consumer (now #{@consumers.size})" }
      notify_observers(:add_consumer, consumer)
    end

    protected def delete_message(sp : SegmentPosition) : Nil
    end
  end
end
