require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    module StreamPosition
      property segment = 1_u32
      property offset = 0_i64
      property pos = 4_u32
      getter requeued = Deque(SegmentPosition).new
    end

    class StreamQueueMessageStore < MessageStore
      getter last_offset = 0_i64
      getter new_messages = Channel(Bool).new
      @offset_header = ::AMQ::Protocol::Table.new({"x-stream-offset" => 0_i64.as(AMQ::Protocol::Field)})

      def initialize(@data_dir : String, @replicator : Replication::Server?)
        super
        set_last_offset
      end

      # Set last_offset to last message offset
      private def set_last_offset : Nil
        bytesize = 0_u32
        mfile = @segments.last_value
        loop do
          bytesize = BytesMessage.skip(mfile)
        rescue IO::EOFError
          break
        end

        begin
          mfile.seek(mfile.pos - bytesize, IO::Seek::Set)
          msg = BytesMessage.from_bytes(mfile.to_slice + mfile.pos)
          @last_offset = offset_from_headers(msg.properties.headers)
        rescue IndexError
        end
      end

      def shift?(consumer : StreamPosition = DefaultPosition::Instance) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
        return if @last_offset <= consumer.offset

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

        loop do
          rfile = @segments[consumer.segment]
          if consumer.pos == rfile.size # EOF?
            rfile.unmap
            consumer.segment = next_read_segment(consumer) || return nil
            consumer.pos = 4_u32
            next
          end
          if deleted?(consumer.segment, consumer.pos)
            rfile.pos = consumer.pos
            BytesMessage.skip(rfile)
            consumer.pos = rfile.pos.to_u32
            next
          end

          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)
          sp = SegmentPosition.make(consumer.segment, consumer.pos, msg)
          consumer.pos += sp.bytesize
          # FIXME: once we've reached the offset there's no need to read it again
          msg_offset = offset_from_headers(msg.properties.headers)
          next if msg_offset < consumer.offset
          consumer.offset = msg_offset

          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(rfile || @segments[consumer.segment], cause: ex)
        end
      end

      private def next_read_segment(consumer) : UInt32?
        @segments.each_key.find { |sid| sid > consumer.segment }
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

      def delete(sp) : Nil
        super
        Log.debug { "Deleting #{sp}" }
        @size -= 1
        @bytesize -= sp.bytesize
        pos = sp.position
        deleted_positions = @deleted[sp.segment]
        # insert sorted
        if idx = deleted_positions.bsearch_index { |dpos| dpos >= pos }
          deleted_positions.insert(idx, pos)
        else
          deleted_positions << pos
        end
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

      private class DefaultPosition
        include StreamPosition

        private def initialize
        end

        Instance = self.new
      end
    end
  end
end
