require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    @durable = true
    @exclusive_consumer = false
    @no_ack = true

    private def init_msg_store(data_dir)
      @msg_store = StreamQueueMessageStore.new(data_dir)
    end

    class StreamQueueMessageStore < MessageStore
      @last_offset = 0_i64

      def initialize(@data_dir : String)
        super
      end

      # Populate bytesize, size and segment_msg_count
      private def load_stats_from_segments : Nil
        last_bytesize = 0_u32
        @segments.each do |seg, mfile|
          count = 0u32
          loop do
            pos = mfile.pos
            raise IO::EOFError.new if pos + BytesMessage::MIN_BYTESIZE >= mfile.size # EOF or a message can't fit, truncate
            ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice + pos)
            break mfile.resize(pos) if ts.zero? # This means that the rest of the file is zero, so resize it

            last_bytesize = bytesize = BytesMessage.skip(mfile)
            count += 1
            unless deleted?(seg, pos)
              @bytesize += bytesize
              @size += 1
            end
          rescue ex : IO::EOFError
            if mfile.pos < mfile.size
              Log.warn { "Truncating #{mfile.path} from #{mfile.size} to #{mfile.pos}" }
              mfile.truncate(mfile.pos)
            end
            break
          rescue ex : OverflowError | AMQ::Protocol::Error::FrameDecode
            raise Error.new(mfile, cause: ex)
          end

          begin # sets @last_offset to last message offset
            mfile.seek(mfile.pos - last_bytesize, IO::Seek::Set)
            msg = BytesMessage.from_bytes(mfile.to_slice + mfile.pos)
            @last_offset = offset_from_headers(msg.properties.headers)
          rescue IndexError
          end

          mfile.pos = 4
          @segment_msg_count[seg] = count
        end
      end

      def shift?(consumer) : Envelope?                              # ameba:disable Metrics/CyclomaticComplexity
        consumer.update_offset(@last_offset) unless consumer.offset # if no offset provided, use offset of last published message
        consumer_offset = consumer.offset || 0_i64
        return if @last_offset <= consumer_offset # don't consume unless there's something to consume

        seg = consumer.segment || @segments.first_value
        pos = consumer.pos || 0_u32
        requeued = consumer.requeued || Deque(SegmentPosition).new
        raise ClosedError.new if @closed
        if sp = requeued.shift?
          segment = @segments[sp.segment]
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
            notify_empty(true) if @size.zero?
            return Envelope.new(sp, msg, redelivered: true)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end

        loop do
          seg = consumer.segment
          rfile = @segments[seg]
          pos = consumer.pos

          if pos == rfile.size # EOF?
            select_next_read_segment
            consumer.update_segment(@rfile_id, 4_u32)
            next
          end
          if deleted?(seg, pos)
            BytesMessage.skip(rfile)
            next
          end
          rfile.pos = pos
          msg = BytesMessage.from_bytes(rfile.to_slice + pos)
          rfile.seek(msg.bytesize, IO::Seek::Current) # seek to next message

          next_pos = pos + msg.bytesize
          consumer.update_segment(@rfile_id, next_pos)
          msg_offset = offset_from_headers(msg.properties.headers)
          next if msg_offset < consumer_offset && next_pos < rfile.size
          consumer.update_offset(msg_offset)

          sp = SegmentPosition.make(seg, pos, msg)
          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(@rfile, cause: ex)
        end
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        offset = @last_offset += 1
        msg = add_offset_header(msg, offset) # save last_index in RAM and update and set it as offset? #how to set first offset on startup/new queue?
        sp = write_to_disk(msg)
        was_empty = @size.zero?
        @bytesize += sp.bytesize
        @size += 1
        notify_empty(false) if was_empty
        sp
      end

      # should delete without ack
      def delete(sp) : Nil
      end

      def add_offset_header(msg, offset)
        headers = msg.properties.headers || ::AMQP::Client::Arguments.new
        headers["x-stream-offset"] = offset.as(AMQ::Protocol::Field)
        msg.properties.headers = headers
        msg
      end

      def offset_from_headers(headers)
        if ht = headers.as?(AMQ::Protocol::Table)
          ht["x-stream-offset"].as(Int64) # TODO: should not be i32, but cast fails
        else
          0_i64
        end
      end
    end

    # save message id / segment position
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

        sp = env.segment_position
        begin
          yield env # deliver the message
        rescue ex   # requeue failed delivery
          consumer.requeue(sp)
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

    def ack(sp : SegmentPosition) : Nil
      return if @deleted
      true
    end

    #do nothing?
    def reject(sp : SegmentPosition, requeue : Bool)
      return if @deleted || @closed
      true
    end


    def add_consumer(consumer : Client::Channel::StreamConsumer)
      return if @closed
      @last_get_time = RoughTime.monotonic
      @consumers_lock.synchronize do
        was_empty = @consumers.empty?
        @consumers << consumer
        notify_consumers_empty(false) if was_empty
      end

      @log.debug { "Adding consumer (now #{@consumers.size})" }
      notify_observers(:add_consumer, consumer)
    end
  end
end
