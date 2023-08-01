require "./stream_queue"

module LavinMQ
  class StreamQueue < Queue
    class StreamQueueMessageStore < MessageStore
      getter new_messages = Channel(Bool).new
      property max_length : Int64?
      property max_length_bytes : Int64?
      property max_age : Time::Span | Time::MonthSpan | Nil
      @last_offset : Int64
      @segment_last_ts = Hash(UInt32, Int64).new(0i64) # used for max-age

      def initialize(@data_dir : String, @replicator : Replication::Server?)
        super
        @last_offset = get_last_offset
        drop_overflow
      end

      private def get_last_offset : Int64
        return 0i64 if @size.zero?
        bytesize = 0_u32
        mfile = @segments.last_value
        loop do
          bytesize = BytesMessage.skip(mfile)
        rescue IO::EOFError
          break
        end

        mfile.seek(mfile.pos - bytesize, IO::Seek::Set)
        msg = BytesMessage.from_bytes(mfile.to_slice + mfile.pos)
        offset_from_headers(msg.properties.headers)
      end

      # Used once when a consumer is started
      # Populates `segment` and `position` by iterating through segments
      # until `offset` is found
      def find_offset(offset) : Tuple(UInt32, UInt32)
        raise ClosedError.new if @closed
        case offset
        when "first", nil then {@segments.first_key, 4u32}
        when "next"       then {@segments.last_key, @segments.last_value.size.to_u32}
        when "last"       then {@segments.last_key, 4u32}
        when Time         then find_offset_in_messages(offset)
        when Int
          if offset >= @last_offset
            {@segments.last_key, @segments.last_value.size.to_u32}
          else
            find_offset_in_messages(offset)
          end
        else raise "Unsupported offset type #{offset.class}"
        end
      end

      private def find_offset_in_messages(offset : Int | Time) : Tuple(UInt32, UInt32)
        segment = @segments.first_key
        pos = 4u32
        loop do
          rfile = @segments[segment]?
          if rfile.nil? || pos == rfile.size
            rfile.unmap if rfile
            segment = @segments.each_key.find { |sid| sid > segment } || break
            rfile = @segments[segment]
            pos = 4_u32
          end
          msg = BytesMessage.from_bytes(rfile.to_slice + pos)
          case offset
          in Int  then break if offset <= offset_from_headers(msg.properties.headers)
          in Time then break if offset <= Time.unix_ms(msg.timestamp)
          end
          pos += msg.bytesize
        rescue ex
          raise rfile ? Error.new(rfile, cause: ex) : ex
        end
        {segment, pos}
      end

      def shift?(consumer : Client::Channel::StreamConsumer) : Envelope?
        raise ClosedError.new if @closed

        if sp = consumer.requeued.shift?
          segment = @segments[sp.segment]
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
            return Envelope.new(sp, msg, redelivered: true)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end

        rfile = @segments[consumer.segment]? || next_segment(consumer) || return nil
        if consumer.pos == rfile.size # EOF
          rfile.unmap
          rfile = next_segment(consumer) || return nil
        end
        begin
          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)
          sp = SegmentPosition.new(consumer.segment, consumer.pos, msg.bytesize.to_u32)
          consumer.pos += sp.bytesize
          Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(rfile, cause: ex)
        end
      end

      private def next_segment(consumer) : MFile?
        if seg_id = @segments.each_key.find { |sid| sid > consumer.segment }
          consumer.segment = seg_id
          consumer.pos = 4u32
          @segments[seg_id]
        else
          consumer.end_of_queue = true
          nil
        end
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        msg = add_offset_header(msg, @last_offset += 1)
        sp = write_to_disk(msg)
        @bytesize += sp.bytesize
        @size += 1
        @segment_last_ts[sp.segment] = msg.timestamp
        @new_messages.try_send? true
        sp
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        drop_overflow
        super
      end

      def drop_overflow
        if max_length = @max_length
          drop_segments_while do
            @size > max_length
          end
        end
        if max_bytes = @max_length_bytes
          drop_segments_while do
            @bytesize > max_bytes
          end
        end
        if max_age = @max_age
          min_ts = RoughTime.utc - max_age
          drop_segments_while do |seg_id|
            last_ts = @segment_last_ts[seg_id]
            Time.unix_ms(last_ts) < min_ts
          end
        end
      end

      private def drop_segments_while(& : UInt32 -> Bool)
        @segments.reject! do |seg_id, mfile|
          break unless yield seg_id
          count = @segment_msg_count.delete(seg_id) || raise KeyError.new
          @segment_last_ts.delete(seg_id)
          @size -= count
          @bytesize -= mfile.size - 4
          mfile.delete.close
          @replicator.try &.delete_file(mfile.path)
          true
        end
      end

      private def update_stat_per_msg(seg, ts)
        @segment_last_ts[seg] = last_ts
      end

      def delete(sp) : Nil
        raise "Only full segments should be deleted"
      end

      private def add_offset_header(msg, offset : Int64)
        if headers = msg.properties.headers
          headers["x-stream-offset"] = offset
        else
          msg.properties.headers = ::AMQ::Protocol::Table.new({"x-stream-offset": offset})
        end
        msg
      end

      private def offset_from_headers(headers) : Int64
        headers.not_nil!("Message lacks headers")["x-stream-offset"].as(Int64)
      end
    end
  end
end
