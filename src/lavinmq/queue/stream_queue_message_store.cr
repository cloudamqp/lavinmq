require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
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

      def shift?(consumer) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
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
            consumer.segment = next_read_segment(consumer) || return nil
            consumer.pos = 4_u32
            next
          end
          Log.debug { "deleted pos in segment=#{@deleted[consumer.segment].size}" }
          if deleted?(consumer.segment, consumer.pos)
            rfile.pos = consumer.pos
            BytesMessage.skip(rfile)
            consumer.pos = rfile.pos.to_u32
            next
          end

          Log.debug { "seg=#{consumer.segment} pos=#{consumer.pos}" }
          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)
          sp = SegmentPosition.make(consumer.segment, consumer.pos, msg)
          consumer.pos += sp.bytesize
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
        offset = @last_offset += 1
        msg = add_offset_header(msg, offset)
        sp = write_to_disk(msg)
        @bytesize += sp.bytesize
        @size += 1
        @new_messages.try_send? true # push to queue for all consumers
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

      def add_offset_header(msg, offset)
        headers = msg.properties.headers || @offset_header
        headers["x-stream-offset"] = offset.as(AMQ::Protocol::Field)
        msg.properties.headers = headers
        msg
      end

      def offset_from_headers(headers)
        headers ? headers["x-stream-offset"].as(Int64) : 0_i64
      end

      private def open_ack_file(id, segment_capacity) : MFile?
        nil
      end
    end
  end
end
