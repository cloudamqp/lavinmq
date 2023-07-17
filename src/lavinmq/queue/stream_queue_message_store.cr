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
          rfile = @segments[consumer.segment] || @segments.first_value

          if consumer.pos == rfile.size # EOF?
            consumer.segment = select_next_read_segment(consumer) || consumer.segment
            consumer.pos = 4_u32
            next
          end
          rfile.pos = consumer.pos

          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)

          msg_offset = offset_from_headers(msg.properties.headers)
          pos = consumer.pos
          consumer.pos += msg.bytesize
          next if msg_offset < consumer.offset
          consumer.offset = msg_offset

          sp = SegmentPosition.make(consumer.segment, pos, msg)
          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(rfile || @segments[consumer.segment], cause: ex)
        end
      end

      private def select_next_read_segment(consumer) : UInt32?
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
