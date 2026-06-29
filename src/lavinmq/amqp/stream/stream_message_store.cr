require "./stream"
require "./stream_consumer"
require "./consumer_offsets"

module LavinMQ::AMQP
  class StreamMessageStore < MessageStore
    # Size bounds for the compacted consumer_offsets file.
    MAX_CONSUMER_OFFSETS_FILE_SIZE = 64_i64 * 1024 * 1024 # 64 MiB
    MIN_CONSUMER_OFFSETS_FILE_SIZE = 8_i64 * 1024         # 8 KiB

    getter new_messages = ::Channel(Bool).new
    property max_length : Int64?
    property max_length_bytes : Int64?
    property max_age : Time::Span | Time::MonthSpan | Nil
    getter last_offset : Int64
    @segment_last_ts = Hash(UInt32, Int64).new(0i64) # used for max-age
    @segment_first_offset = Hash(UInt32, Int64).new  # segment_id => offset of first msg
    @segment_first_ts = Hash(UInt32, Int64).new      # segment_id => ts of first msg
    @consumer_offsets : MFile
    @consumer_offset_positions = Hash(String, Int64).new # used for consumer offsets

    def initialize(*args, **kwargs)
      super
      @last_offset = get_last_offset
      @consumer_offsets = MFile.new(File.join(@msg_dir, "consumer_offsets"), Config.instance.segment_size)
      @replicator.try &.register_file @consumer_offsets
      @consumer_offset_positions = restore_consumer_offset_positions
      drop_overflow
    end

    def close : Nil
      super
      @consumer_offsets.close
    end

    def delete
      super
      delete_file(@consumer_offsets)
    end

    private def get_last_offset : Int64
      return 0i64 if @size.zero?
      offset = @segment_first_offset.last_value
      # to_i64 first: when the trailing segment was opened but has no messages
      # yet (4-byte schema header only) @segment_msg_count is 0_u32 and the
      # subtraction would underflow UInt32. -1 here means "one before this
      # segment's first offset", which is the last assigned offset.
      offset += @segment_msg_count.last_value.to_i64 - 1
      offset
    end

    # Used once when a consumer is started
    # Populates `segment` and `position` by iterating through segments
    # until `offset` is found
    # ameba:disable Metrics/CyclomaticComplexity
    def find_offset(offset, tag = nil, track_offset = false) : Tuple(Int64, UInt32, UInt32)
      raise ClosedError.new if @closed
      if track_offset
        consumer_last_offset = last_offset_by_consumer_tag(tag)
        return find_offset_in_segments(consumer_last_offset) if consumer_last_offset
      end

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
      else raise OffsetError.new(offset)
      end
    end

    def unmap_segments(except : Enumerable(UInt32) = StaticArray(UInt32, 0).new(0u32))
      @segments.each do |seg_id, mfile|
        next if mfile == @wfile
        next if except.includes? seg_id
        mfile.dontneed
      end
    end

    private def offset_at(seg, pos, retried = false) : Tuple(Int64, UInt32, UInt32)
      return {@last_offset, seg, pos} if @size.zero?
      mfile = @segments[seg]
      offset = @segment_first_offset[seg]
      mfile.pos = 4
      while mfile.pos < pos
        BytesMessage.skip(mfile)
        offset += 1
      end
      {offset, seg, pos}
    rescue ex : IndexError # first segment can be empty if message size >= segment size
      return offset_at(seg + 1, 4_u32, true) unless retried
      raise ex
    end

    private def last_offset_seg_pos
      {@last_offset + 1, @segments.last_key, @segments.last_value.size.to_u32}
    end

    private def find_offset_in_segments(offset : Int | Time) : Tuple(Int64, UInt32, UInt32)
      segment = offset_index_lookup(offset)
      pos = 4u32
      msg_offset = @segment_first_offset[segment] || 0i64
      loop do
        rfile = @segments[segment]?
        if rfile.nil? || pos == rfile.size
          if segment = @segments.each_key.find { |sid| sid > segment }
            rfile = @segments[segment]
            pos = 4u32
            msg_offset = @segment_first_offset[segment]
          else
            return last_offset_seg_pos
          end
        end
        msg = BytesMessage.from_bytes(rfile.to_slice + pos)

        case offset
        in Int  then break if offset <= msg_offset
        in Time then break if offset <= Time.unix_ms(msg.timestamp)
        end
        msg_offset += 1
        pos += msg.bytesize.to_u32
      rescue ex
        raise rfile ? Error.new(rfile, cause: ex) : ex
      end
      {msg_offset, segment, pos}
    end

    private def offset_index_lookup(offset) : UInt32
      seg = @segments.first_key
      case offset
      when Int
        @segment_first_offset.each do |seg_id, first_seg_offset|
          break if first_seg_offset > offset
          seg = seg_id
        end
      when Time
        @segment_first_ts.each do |seg_id, first_seg_ts|
          break if Time.unix_ms(first_seg_ts) > offset
          seg = seg_id
        end
      end
      seg
    end

    def last_offset_by_consumer_tag(consumer_tag)
      if pos = @consumer_offset_positions[consumer_tag]?
        tx = @consumer_offsets.to_slice(pos, 8)
        return IO::ByteFormat::LittleEndian.decode(Int64, tx)
      end
    end

    private def restore_consumer_offset_positions : Hash(String, Int64)
      positions = Hash(String, Int64).new
      return positions if @consumer_offsets.size.zero?

      loop do
        ctag = AMQ::Protocol::ShortString.from_io(@consumer_offsets)
        break if ctag.empty?
        positions[ctag] = @consumer_offsets.pos
        @consumer_offsets.skip(8)
      rescue IO::EOFError
        break
      end
      @consumer_offsets.pos = 0 if @consumer_offsets.pos == 1
      @consumer_offsets.resize(@consumer_offsets.pos)
      positions
    end

    def store_consumer_offset(consumer_tag : String, new_offset : Int64)
      raise ClosedError.new if @closed
      cleanup_consumer_offsets if consumer_offset_file_full?(consumer_tag)
      write_consumer_offset(consumer_tag, new_offset)
    end

    private def write_consumer_offset(consumer_tag : String, new_offset : Int64)
      start_pos = @consumer_offsets.size
      @consumer_offsets.write_bytes AMQ::Protocol::ShortString.new(consumer_tag)
      @consumer_offset_positions[consumer_tag] = @consumer_offsets.size
      @consumer_offsets.write_bytes new_offset
      @replicator.try &.append(@consumer_offsets.path, start_pos, ConsumerOffsets.entry_size(consumer_tag))
    end

    def consumer_offset_file_full?(consumer_tag)
      (@consumer_offsets.size + ConsumerOffsets.entry_size(consumer_tag)) >= @consumer_offsets.capacity
    end

    def cleanup_consumer_offsets
      return if @consumer_offsets.size.zero?

      lowest_offset_in_stream, _seg, _pos = offset_at(@segments.first_key, 4u32)

      # Offsets still within the stream (higher position == more recently committed).
      tracked_offsets = Array(Tuple(String, Int64, Int64)).new
      @consumer_offset_positions.each do |ctag, pos|
        if (offset = last_offset_by_consumer_tag(ctag)) && offset >= lowest_offset_in_stream
          tracked_offsets << {ctag, offset, pos}
        end
      end

      # Reserve room for one max-length entry so the append that triggered
      # cleanup always fits in the rewritten file.
      budget = MAX_CONSUMER_OFFSETS_FILE_SIZE - ConsumerOffsets::MAX_ENTRY_SIZE
      offsets_to_save = ConsumerOffsets.trim_to_size(tracked_offsets, budget)
      if (dropped = tracked_offsets.size - offsets_to_save.size) > 0
        Log.warn { "Consumer offsets file for #{@msg_dir} is full, dropped #{dropped} oldest consumer offset(s)" }
      end
      used_bytes = offsets_to_save.sum { |ctag, _offset, _pos| ConsumerOffsets.entry_size(ctag) }
      # Allocate 1000x the used size as headroom for future appends, bounded by the min/max file size.
      new_capacity = (used_bytes * 1000).clamp(MIN_CONSUMER_OFFSETS_FILE_SIZE, MAX_CONSUMER_OFFSETS_FILE_SIZE)
      replace_offsets_file(new_capacity, offsets_to_save)
    end

    private def replace_offsets_file(capacity : Int, offsets_to_save : Array(Tuple(String, Int64, Int64)))
      old_consumer_offsets = @consumer_offsets
      @consumer_offset_positions = Hash(String, Int64).new
      @consumer_offsets = MFile.new("#{old_consumer_offsets.path}.tmp", capacity)
      offsets_to_save.each do |consumer_tag, offset, _pos|
        @consumer_offsets.write_byte consumer_tag.bytesize.to_u8
        @consumer_offsets.write consumer_tag.to_slice
        @consumer_offset_positions[consumer_tag] = @consumer_offsets.size
        @consumer_offsets.write_bytes offset
      end
      @consumer_offsets.rename(old_consumer_offsets.path)
      @replicator.try &.replace_file(@consumer_offsets)
      old_consumer_offsets.close(truncate_to_size: false)
    end

    def read(segment : UInt32, position : UInt32) : Envelope?
      return if @closed
      rfile = @segments[segment]
      return if position == rfile.size
      begin
        msg = BytesMessage.from_bytes(rfile.to_slice + position)
        sp = SegmentPosition.new(segment, position, msg.bytesize.to_u32)
        Envelope.new(sp, msg, redelivered: false)
      rescue ex
        puts "read segment=#{segment} position=#{position}"
        raise Error.new(rfile, cause: ex)
      end
    end

    def shift?(consumer : AMQP::StreamConsumer) : Envelope?
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
        msg.properties.headers = add_offset_header(msg.properties.headers, consumer.offset)
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
            offset, _, _ = offset_at(sp.segment, sp.position)
            msg.properties.headers = add_offset_header(msg.properties.headers, offset)
            return Envelope.new(sp, msg, redelivered: true)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end
      end
    end

    def next_segment_id(segment) : UInt32?
      @segments.each_key.find { |sid| sid > segment }
    end

    private def next_segment(consumer) : MFile?
      if seg_id = next_segment_id(consumer.segment)
        consumer.segment = seg_id
        consumer.pos = 4u32
        @segments[seg_id]
      end
    end

    def push(msg) : SegmentPosition
      raise ClosedError.new if @closed
      @last_offset += 1
      sp = write_to_disk(msg)
      @bytesize += sp.bytesize
      @size += 1
      @segment_last_ts[sp.segment] = msg.timestamp
      sp
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      super.tap do
        drop_overflow
        @segment_first_offset[@segments.last_key] = @last_offset.zero? ? 1i64 : @last_offset
        @segment_first_ts[@segments.last_key] = RoughTime.unix_ms
      end
    end

    private def write_metadata(io, seg)
      super
      io.write_bytes @segment_first_offset[seg]
      io.write_bytes @segment_first_ts[seg]
      io.write_bytes @segment_last_ts[seg]
    end

    def drop_overflow
      return if @closed
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
      cleanup_consumer_offsets
    end

    private def drop_segments_while(& : UInt32 -> Bool)
      @segments.reject! do |seg_id, mfile|
        should_drop = yield seg_id
        break unless should_drop
        next if mfile == @wfile # never delete the last active segment
        msg_count = @segment_msg_count.delete(seg_id)
        @size -= msg_count if msg_count
        @segment_last_ts.delete(seg_id)
        @segment_first_offset.delete(seg_id)
        @segment_first_ts.delete(seg_id)
        @bytesize -= mfile.size - 4
        delete_file(mfile, including_meta: true)
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

    private def produce_metadata(seg, mfile)
      super
      if empty? || @segment_msg_count[seg].zero?
        # Whole queue empty, or this trailing segment was opened (4-byte
        # Schema::VERSION header written by open_new_segment) but no message
        # was written before shutdown — don't parse a msg out of an empty file.
        previous_segment_first_offset = @segment_first_offset[seg - 1]? || 1i64
        previous_segment_msg_count = @segment_msg_count[seg - 1]? || 0i64
        @segment_first_offset[seg] = previous_segment_first_offset + previous_segment_msg_count
        @segment_first_ts[seg] = RoughTime.unix_ms
        @segment_last_ts[seg] = RoughTime.unix_ms
      else
        previous_segment_first_offset = @segment_first_offset[seg - 1]? || 1i64
        previous_segment_msg_count = @segment_msg_count[seg - 1]? || 0i64
        msg = BytesMessage.from_bytes(mfile.to_slice + 4u32)
        @segment_first_offset[seg] = previous_segment_first_offset + previous_segment_msg_count
        @segment_first_ts[seg] = msg.timestamp
        # NOTE: scan_last_ts re-scans the segment even though super already did.
        # This path only runs when metadata files are missing, so the cost is acceptable.
        @segment_last_ts[seg] = scan_last_ts(mfile)
      end
    end

    private def read_extra_metadata_fields(file : File, seg : UInt32)
      stored_offset = file.read_bytes(Int64)
      @segment_first_ts[seg] = file.read_bytes(Int64)

      begin
        @segment_last_ts[seg] = file.read_bytes(Int64)
      rescue IO::EOFError
        # Old metadata format without last_ts, scan segment to find it
        @log.warn { "Metadata for segment #{seg} is missing last_ts, scanning segment to determine it" }
        @segment_last_ts[seg] = scan_last_ts(@segments[seg])
        write_metadata_file(seg, @segments[seg])
      end

      # Validate and fix (possibly) incorrect offsets from existing metadata
      @segment_first_offset[seg] = if seg == 1u32
                                     1i64 # First segment always starts at offset 1
                                   elsif prev_first = @segment_first_offset[seg - 1]?
                                     # Calculate based on previous segment
                                     prev_count = @segment_msg_count[seg - 1]? || 0i64
                                     prev_first + prev_count
                                   else
                                     stored_offset # No previous segment info, use stored value
                                   end
    end

    private def scan_last_ts(mfile) : Int64
      last_ts = 0i64
      mfile.pos = 4
      while mfile.pos < mfile.size
        last_ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(mfile.pos, 8))
        BytesMessage.skip(mfile)
      end
      last_ts
    end

    class OffsetError < Exception
      def initialize(offset)
        super("invalid offset #{offset}")
      end
    end
  end
end
