require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    @durable = true
    @exclusive_consumer = false
    @no_ack = true
    @last_offset = 0_i64

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
        if sp = requeued.shift?
          segment = @segments[sp.segment]
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
          seg = if consumer.segment
                  consumer.segment
                else
                  @rfile_id
                end
          rfile = @segments[seg]
          pos = if consumer.pos
                  consumer.pos
                else
                  rfile.pos.to_u32
                end

          pos ||= 0_u32
          seg ||= 0_u32

          if pos == rfile.size # EOF?
            select_next_read_segment
            puts "consumer  #{consumer.segment} #{consumer.pos}"
            consumer.update_segment(@rfile_id, 0_u32)
            pos = 0_u32
            seg = @rfile_id
            next unless @size.zero?
            return if @size.zero?
            # raise IO::EOFError.new("EOF but @size=#{@size}")
          end
          if deleted?(seg, pos)
            BytesMessage.skip(rfile)
            next
          end
          msg = BytesMessage.from_bytes(rfile.to_slice + pos)

          rfile.seek(msg.bytesize, IO::Seek::Current)
          @bytesize -= msg.bytesize

          offset = consumer.offset || 0_i64
          msg_offset = 0
          headers = msg.properties.headers
          if ht = headers.as?(AMQ::Protocol::Table)
            msg_offset = ht["x-stream-offset"].as(Int32)
          end
          if msg_offset < offset
            puts "message.offset #{msg_offset} < offset #{offset}, next"
            next_pos = pos + msg.bytesize
            consumer.update_segment(@rfile_id, next_pos)
            next unless next_pos >= rfile.size
          end

          sp = SegmentPosition.make(seg, pos, msg)
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
        consumer.update_offset(@last_offset) unless consumer.offset # if no offset provided, use offset of last published message
        # TODO: @last_offset is 0 when starting lavin, we need to set it to last message offset in queue

        env = @msg_store_lock.synchronize { @msg_store.shift?(consumer) } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end

        puts "env.message.offset: #{env.message.offset}"
        headers = env.message.properties.headers
        msg_offset = 0
        if ht = headers.as?(AMQ::Protocol::Table)
          msg_offset = ht["x-stream-offset"].as(Int32).to_i64 # TODO: should not be i32, but cast fails
          puts "msg_offset: #{msg_offset}"
          consumer.update_offset(msg_offset)
        end

        sp = env.segment_position
        if consumer.no_ack
          begin
            yield env # deliver the message
          rescue ex   # requeue failed delivery
            consumer.requeue(sp)
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
