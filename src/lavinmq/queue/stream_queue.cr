require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    @durable = true
    @exclusive_consumer = false
    @no_ack = true
    @last_offset = 0
    @consumers = [] of Client::Channel::Consumer

    private def init_msg_store(data_dir)
      @msg_store = StreamQueueMessageStore.new(data_dir)
    end

    class StreamConsumer < LavinMQ::Client::Channel::Consumer
      @offset = 0_u64
      @last_offset = 0_u64
      @current_segment = MFile
      @pos = 0_u64

      private def deliver_loop
        queue = @queue
        no_ack = @no_ack
        offset = @offset
        i = 0
        loop do
          wait_for_capacity
          loop do
            raise ClosedError.new if @closed
            next if wait_for_global_capacity
            next if wait_for_priority_consumers
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          queue.consume_get(no_ack, offset, @current_segment) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue ex : ClosedError | Queue::ClosedError | Client::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      end
    end

    class StreamQueueMessageStore < MessageStore
      def initialize(@data_dir : String)
        super
        # message id? segment position?
        # @last_offset = last message offset
      end

      def shift? : Envelope? # ameba:disable Metrics/CyclomaticComplexity
        raise ClosedError.new if @closed
        if sp = @requeued.shift?
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
          rfile = @rfile
          seg = @rfile_id
          pos = rfile.pos.to_u32
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
    def consume_get(no_ack, offset, current_segment, & : Envelope -> Nil) : Bool
      get(no_ack, offset, current_segment) do |env|
        yield env
        env.redelivered ? (@redeliver_count += 1) : (@deliver_count += 1)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(no_ack, offset, current_segment,   & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      loop do # retry if msg expired or deliver limit hit
        env = @msg_store_lock.synchronize { @msg_store.shift?(offset, current_segment) } || break
        if has_expired?(env.message) # guarantee to not deliver expired messages
          expire_msg(env, :expired)
          next
        end
        puts "::::get::::"
        puts "env.message.offset: #{env.message.offset}"
        puts "offset: #{offset}"
        headers = env.message.properties.headers

        msg_offset=0
        if ht = headers.as?(AMQ::Protocol::Table)
          msg_offset = ht["x-stream-offset"].as(Int64)
          puts "msg_offset: #{msg_offset}"
        end

        # some testing for finding the right message by offset, should be per consumer
        # and we should probably also save segment/position per consumer
        # but we also need a "seek" if the offset is changed or a new consumer is added
        if offset && msg_offset < offset.as(UInt64)
          puts "env.message.offset #{env.message.offset} < offset #{offset}}"
          next
        end
        sp = env.segment_position
        if no_ack
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
    def add_consumer(consumer : Client::Channel::Consumer)
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

    private def handle_arguments
      @dlx = parse_header("x-dead-letter-exchange", String)
      @dlrk = parse_header("x-dead-letter-routing-key", String)
      if @dlrk && @dlx.nil?
        raise LavinMQ::Error::PreconditionFailed.new("x-dead-letter-exchange required if x-dead-letter-routing-key is defined")
      end
      @expires = parse_header("x-expires", Int).try &.to_i64
      validate_gt_zero("x-expires", @expires)
      @queue_expiration_ttl_change.try_send? nil
      @max_length = parse_header("x-max-length", Int).try &.to_i64
      validate_positive("x-max-length", @max_length)
      @max_length_bytes = parse_header("x-max-length-bytes", Int).try &.to_i64
      validate_positive("x-max-length-bytes", @max_length_bytes)
      @message_ttl = parse_header("x-message-ttl", Int).try &.to_i64
      validate_positive("x-message-ttl", @message_ttl)
      @message_ttl_change.try_send? nil
      @delivery_limit = parse_header("x-delivery-limit", Int).try &.to_i64
      validate_positive("x-delivery-limit", @delivery_limit)
      @reject_on_overflow = parse_header("x-overflow", String) == "reject-publish"
    end

    # do nothing
    def reject(sp : SegmentPosition, requeue : Bool)
    end

    # do nothing
    def ack(sp : SegmentPosition) : Nil
    end

    protected def delete_message(sp : SegmentPosition) : Nil
    end
  end
end
