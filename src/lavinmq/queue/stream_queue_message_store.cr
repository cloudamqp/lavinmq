require "./durable_queue"

module LavinMQ
  class StreamQueue < Queue
    module StreamPosition
      property offset = 0_i64
      property segment = 1_u32
      property pos = 4_u32
      getter requeued = Deque(SegmentPosition).new
    end

    class StreamQueueMessageStore < MessageStore
      getter last_offset = 0_i64
      getter new_messages = Channel(Bool).new
      property max_length : Int64? = nil
      property max_length_bytes : Int64? = nil

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
      def find_offset(offset : Int64) : Tuple(UInt32, UInt32)
        raise ClosedError.new if @closed
        if @last_offset <= offset
          segment = @segments.last_key
          pos = @segments.last_value.size.to_u32
          return {segment, pos}
        end
        segment = @segments.first_key
        pos = 4_u32
        loop do
          rfile = @segments[segment]?
          if rfile.nil? || pos == rfile.size
            rfile.unmap if rfile
            segment = next_read_segment(segment) || break
            pos = 4_u32
            next
          end

          msg = BytesMessage.from_bytes(rfile.to_slice + pos)
          sp = SegmentPosition.new(segment, pos, msg.bytesize.to_u32)
          msg_offset = offset_from_headers(msg.properties.headers)
          break if msg_offset >= offset
          pos += sp.bytesize
        rescue ex
          raise rfile ? Error.new(rfile, cause: ex) : ex
        end
        {segment, pos}
      end

      def shift?(consumer : StreamPosition) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
        raise ClosedError.new if @closed
        return if @last_offset <= consumer.offset

        if sp = consumer.requeued.shift?
          segment = @segments[sp.segment]
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
            return Envelope.new(sp, msg, redelivered: true)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end

        loop do
          rfile = @segments[consumer.segment]?
          if rfile.nil? || consumer.pos == rfile.size
            rfile.unmap if rfile
            consumer.segment = next_read_segment(consumer.segment) || return nil
            consumer.pos = 4_u32
            next
          end
          # only head is deleted, no need to check if msgs are deleted in the middle
          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)
          sp = SegmentPosition.new(consumer.segment, consumer.pos, msg.bytesize.to_u32)
          consumer.pos += sp.bytesize
          consumer.offset += 1

          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise rfile ? Error.new(rfile, cause: ex) : ex
        end
      end

      private def next_read_segment(segment) : UInt32?
        @segments.each_key.find { |sid| sid > segment }
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        msg = add_offset_header(msg, @last_offset += 1)
        sp = write_to_disk(msg)
        @bytesize += sp.bytesize
        @size += 1
        @new_messages.try_send? true
        sp
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        drop_overflow
        super
      end

      def drop_overflow
        if ml = @max_length
          @segments.reject! do |seg_id, mfile|
            break if ml >= @size || mfile == @wfile
            count = @segment_msg_count.delete(seg_id) || raise KeyError.new
            @size -= count
            @bytesize -= mfile.size
            mfile.delete.close
            @replicator.try &.delete_file(mfile.path)
            true
          end
        end

        if mlb = @max_length_bytes
          @segments.reject! do |seg_id, mfile|
            break if mlb >= @bytesize || mfile == @wfile
            count = @segment_msg_count.delete(seg_id) || raise KeyError.new
            @size -= count
            @bytesize -= mfile.size
            mfile.delete.close
            @replicator.try &.delete_file(mfile.path)
            true
          end
        end
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
