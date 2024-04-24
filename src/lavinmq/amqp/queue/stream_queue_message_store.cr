require "./stream_queue"
require "../stream_consumer"

module LavinMQ::AMQP
  class StreamQueue < DurableQueue
    class StreamQueueMessageStore < MessageStore
      getter new_messages = ::Channel(Bool).new
      property max_length : Int64?
      property max_length_bytes : Int64?
      property max_age : Time::Span | Time::MonthSpan | Nil
      getter last_offset : Int64
      @segment_last_ts = Hash(UInt32, Int64).new(0i64) # used for max-age
      @offset_index = Hash(UInt32, Int64).new          # segment_id => offset of first msg
      @timestamp_index = Hash(UInt32, Int64).new       # segment_id => ts of first msg
      @consumer_offset_path : String
      @consumer_offsets : MFile?
      @consumer_offset_positions = Hash(String, Int64).new # used for consumer offsets
      @consumer_offset_capacity = 32_768

      def initialize(*args, **kwargs)
        super
        @last_offset = get_last_offset
        build_segment_indexes
        @consumer_offset_path = File.join(@queue_data_dir, "consumer_offsets")
        @consumer_offsets = MFile.new(@consumer_offset_path, @consumer_offset_capacity)
        @consumer_offset_positions = consumer_offset_positions
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
        msg = BytesMessage.from_bytes(mfile.to_slice + (mfile.pos - bytesize))
        offset_from_headers(msg.properties.headers)
      end

      # Used once when a consumer is started
      # Populates `segment` and `position` by iterating through segments
      # until `offset` is found
      def find_offset(offset, tag = nil) : Tuple(Int64, UInt32, UInt32)
        raise ClosedError.new if @closed
        case offset
        when "first" then offset_at(@segments.first_key, 4u32)
        when "last"  then offset_at(@segments.last_key, 4u32)
        when "next"  then last_offset_seg_pos
        when Time    then find_offset_in_segments(offset)
        when nil
          consumer_last_offset = last_offset_by_consumer_tag(tag) || 0
          find_offset_in_segments(consumer_last_offset)
        when Int
          if offset > @last_offset
            last_offset_seg_pos
          else
            find_offset_in_segments(offset)
          end
        else raise "Invalid offset parameter: #{offset}"
        end
      end

      private def offset_at(seg, pos) : Tuple(Int64, UInt32, UInt32)
        return {@last_offset, seg, pos} if @size.zero?
        mfile = @segments[seg]
        msg = BytesMessage.from_bytes(mfile.to_slice + pos)
        offset = offset_from_headers(msg.properties.headers)
        {offset, seg, pos}
      end

      private def last_offset_seg_pos
        {@last_offset + 1, @segments.last_key, @segments.last_value.size.to_u32}
      end

      private def find_offset_in_segments(offset : Int | Time) : Tuple(Int64, UInt32, UInt32)
        segment = offset_index_lookup(offset)
        pos = 4u32
        msg_offset = 0i64
        loop do
          rfile = @segments[segment]?
          if rfile.nil? || pos == rfile.size
            if segment = @segments.each_key.find { |sid| sid > segment }
              rfile = @segments[segment]
              pos = 4_u32
            else
              return last_offset_seg_pos
            end
          end
          msg = BytesMessage.from_bytes(rfile.to_slice + pos)
          msg_offset = offset_from_headers(msg.properties.headers)
          case offset
          in Int  then break if offset <= msg_offset
          in Time then break if offset <= Time.unix_ms(msg.timestamp)
          end
          pos += msg.bytesize
        rescue ex
          raise rfile ? Error.new(rfile, cause: ex) : ex
        end
        {msg_offset, segment, pos}
      end

      private def offset_index_lookup(offset) : UInt32
        seg = @segments.first_key
        case offset
        when Int
          @offset_index.each do |seg_id, first_seg_offset|
            break if first_seg_offset > offset
            seg = seg_id
          end
        when Time
          @timestamp_index.each do |seg_id, first_seg_ts|
            break if Time.unix_ms(first_seg_ts) > offset
            seg = seg_id
          end
        end
        seg
      end

      def last_offset_by_consumer_tag(consumer_tag)
        if pos = @consumer_offset_positions[consumer_tag]?
          tx = @consumer_offsets.to_slice(pos, 8)
          return IO::ByteFormat::SystemEndian.decode(Int64, tx)
        end
      end

      def consumer_offsets : MFile
        return @consumer_offsets.not_nil! if @consumer_offsets && !@consumer_offsets.not_nil!.closed?
        path = File.join(@data_dir, "consumer_offsets")
        @consumer_offsets = MFile.new(path, 5000) # TODO: size?
      end

      private def consumer_offset_positions
        positions = Hash(String, Int64).new
        slice = @consumer_offsets.to_slice
        return positions if slice.size.zero?
        pos = 0

        loop do
          ctag_length = IO::ByteFormat::LittleEndian.decode(UInt8, slice[pos, pos + 1])
          break if ctag_length == 0
          pos += 1
          ctag = String.new(slice[pos..pos + ctag_length - 1])
          pos += ctag_length
          positions[ctag] = pos
          pos += 8
          break if pos >= slice.size
        end
        @consumer_offsets.resize(pos) # resize mfile to remove any empty bytes
        positions
      end

      def save_offset_by_consumer_tag(consumer_tag, new_offset)
        pos = 0_i64
        begin
          if pos = @consumer_offset_positions[consumer_tag]?
            buf = uninitialized UInt8[8]
            IO::ByteFormat::LittleEndian.encode(new_offset.as(Int64), buf.to_slice)
            @consumer_offsets.write_at(pos, buf.to_slice)
          else
            write_new_ctag_to_file(consumer_tag, new_offset)
          end
        end
      end

      def write_new_ctag_to_file(consumer_tag, new_offset)
        slice = consumer_tag.to_slice
        consumer_tag_length = slice.size.to_u8
        pos = @consumer_offsets.size + slice.size + 1

        length_buffer = uninitialized UInt8[1]
        IO::ByteFormat::LittleEndian.encode(consumer_tag_length, length_buffer.to_slice)

        offset_buffer = uninitialized UInt8[8]
        IO::ByteFormat::LittleEndian.encode(new_offset.as(Int64), offset_buffer.to_slice)

        @consumer_offsets.write(length_buffer.to_slice + slice + offset_buffer.to_slice)
        @consumer_offset_positions[consumer_tag] = pos
      end

      def cleanup_consumer_offsets
        offsets_to_save = Hash(String, Int64).new
        lowest_offset_in_stream, _seg, _pos = offset_at(@segments.first_key, 4u32) # handle
        @consumer_offset_positions.each do |ctag, pos|
          offset = last_offset_by_consumer_tag(ctag).not_nil!
          next if offset < lowest_offset_in_stream
          # Other scenarios to remove?
          offsets_to_save[ctag] = offset
        end

        delete_and_reopen_offsets_file
        @consumer_offset_positions = Hash(String, Int64).new
        offsets_to_save.each do |ctag, offset|
          write_new_ctag_to_file(ctag, offset)
        end
      end

      def remove_consumer_tag_from_file(consumer_tag)
        @consumer_offset_positions = @consumer_offset_positions.reject! { |k, _v| k == consumer_tag }

        offsets_to_save = Hash(String, Int64).new
        @consumer_offset_positions.each do |ctag, _p|
          offset = last_offset_by_consumer_tag(ctag)
          next unless offset
          offsets_to_save[ctag] = offset
        end

        delete_and_reopen_offsets_file
        offsets_to_save.each do |ctag, offset|
          write_new_ctag_to_file(ctag, offset)
        end
      end

      def delete_and_reopen_offsets_file
        @consumer_offsets.close
        @consumer_offsets.delete
        @consumer_offsets = MFile.new(@consumer_offset_path, @consumer_offset_capacity)
      end

      def shift?(consumer : Client::Channel::StreamConsumer) : Envelope?
        raise ClosedError.new if @closed

        if env = shift_requeued(consumer.requeued)
          return env
        end

        return if consumer.offset > @last_offset
        rfile = @segments[consumer.segment]? || next_segment(consumer) || return
        if consumer.pos == rfile.size # EOF
          return if rfile == @wfile
          rfile = next_segment(consumer) || return
        end
        begin
          msg = BytesMessage.from_bytes(rfile.to_slice + consumer.pos)
          sp = SegmentPosition.new(consumer.segment, consumer.pos, msg.bytesize.to_u32)
          consumer.pos += sp.bytesize
          consumer.offset += 1
          return unless consumer.filter_match?(msg.properties.headers)
          Envelope.new(sp, msg, redelivered: false)
        rescue ex
          raise Error.new(rfile, cause: ex)
        end
      end

      private def shift_requeued(requeued) : Envelope?
        while sp = requeued.shift?
          if segment = @segments[sp.segment]? # segment might have expired since requeued
            begin
              msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
              return Envelope.new(sp, msg, redelivered: true)
            rescue ex
              raise Error.new(segment, cause: ex)
            end
          end
        end
      end

      private def next_segment(consumer) : MFile?
        if seg_id = @segments.each_key.find { |sid| sid > consumer.segment }
          consumer.segment = seg_id
          consumer.pos = 4u32
          @segments[seg_id]
        end
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        msg.properties.headers = add_offset_header(msg.properties.headers, @last_offset += 1)
        sp = write_to_disk(msg)
        @bytesize += sp.bytesize
        @size += 1
        @segment_last_ts[sp.segment] = msg.timestamp
        @new_messages.try_send? true
        sp
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        super.tap do
          drop_overflow
          @offset_index[@segments.last_key] = @last_offset + 1
          @timestamp_index[@segments.last_key] = RoughTime.unix_ms
        end
      end

      def drop_overflow
        if max_length = @max_length
          drop_segments_while do
            @size >= max_length
          end
        end
        if max_bytes = @max_length_bytes
          drop_segments_while do
            @bytesize >= max_bytes
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
          should_drop = yield seg_id
          break unless should_drop
          next if mfile == @wfile # never delete the last active segment
          msg_count = @segment_msg_count.delete(seg_id)
          @size -= msg_count if msg_count
          @segment_last_ts.delete(seg_id)
          @offset_index.delete(seg_id)
          @timestamp_index.delete(seg_id)
          @bytesize -= mfile.size - 4
          mfile.delete.close
          @replicator.try &.delete_file(mfile.path)
          true
        end
      end

      def purge(max_count : Int = UInt32::MAX) : UInt32
        raise ClosedError.new if @closed
        start_size = @size
        count = 0u32
        drop_segments_while do |seg_id|
          max_count >= (count += @segment_msg_count[seg_id])
        end
        start_size - @size
      end

      def delete(sp) : Nil
        raise "Only full segments should be deleted"
      end

      private def add_offset_header(headers, offset : Int64) : AMQP::Table
        if headers
          headers["x-stream-offset"] = offset
          headers
        else
          AMQP::Table.new({"x-stream-offset": offset})
        end
      end

      private def offset_from_headers(headers) : Int64
        headers.not_nil!("Message lacks headers")["x-stream-offset"].as(Int64)
      end

      private def build_segment_indexes
        @segments.each do |seg_id, mfile|
          msg = BytesMessage.from_bytes(mfile.to_slice + 4u32)
          @offset_index[seg_id] = offset_from_headers(msg.properties.headers)
          @timestamp_index[seg_id] = msg.timestamp
        rescue IndexError
          @offset_index[seg_id] = @last_offset
          @timestamp_index[seg_id] = RoughTime.unix_ms
        end
      end
    end
  end
end
