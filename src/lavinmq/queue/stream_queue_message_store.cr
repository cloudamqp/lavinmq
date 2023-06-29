require "./durable_queue"
require "../client/channel/consumer"

module LavinMQ
  class StreamQueue < Queue
    class StreamQueueMessageStore < MessageStore
      @last_offset = 0_i64
      getter last_offset
      @new_messages = Channel(Bool).new
      getter new_messages

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

      def empty?(consumer)
        return true if @size.zero?
        @last_offset <= consumer.offset
      end

      def shift?(consumer) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
        return if @last_offset <= consumer.offset

        seg = consumer.segment || @segments.first_value
        pos = consumer.pos || 0_u32
        requeued = consumer.requeued
        raise ClosedError.new if @closed
        if sp = requeued.shift?
          segment = @segments[sp.segment]
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
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
            consumer.segment = @rfile_id
            consumer.pos = 4_u32
            rfile = @segments[@rfile_id]
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
          consumer.pos = next_pos
          msg_offset = offset_from_headers(msg.properties.headers)
          next if msg_offset < consumer.offset
          consumer.offset = msg_offset

          sp = SegmentPosition.make(seg, pos, msg)
          return Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(@rfile, cause: ex)
        end
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
        headers = msg.properties.headers || ::AMQP::Client::Arguments.new
        headers["x-stream-offset"] = offset.as(AMQ::Protocol::Field)
        msg.properties.headers = headers
        msg
      end

      def offset_from_headers(headers)
        if ht = headers.as?(AMQ::Protocol::Table)
          ht["x-stream-offset"].as(Int64)
        else
          0_i64
        end
      end
    end
  end
end
