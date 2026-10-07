require "./stream"
require "./stream_consumer"
require "./consumer_offsets"
require "./stream_offset"

module LavinMQ::AMQP
  class StreamMessageStore < MessageStore
    getter new_messages = ::Channel(Bool).new
    # Signalled when the next max-age expiry may have moved: a new segment was
    # opened or max-age changed. Closed with the store.
    getter expiry_changed = ::Channel(Nil).new(1)
    property max_length : Int64?
    property max_length_bytes : Int64?
    property max_age : (Time::Span | Time::MonthSpan)?
    getter last_offset : Int64
    @segment_last_ts = Hash(UInt32, Int64).new(0i64) # used for max-age
    @segment_first_offset = Hash(UInt32, Int64).new  # segment_id => offset of first msg
    @segment_first_ts = Hash(UInt32, Int64).new      # segment_id => ts of first msg
    @consumer_offsets : ConsumerOffsets
    @segment_readers = Hash(UInt32, UInt32).new # segment_id => consumers positioned in it

    def initialize(*args, **kwargs)
      super
      @last_offset = get_last_offset
      @consumer_offsets = ConsumerOffsets.new(@msg_dir, Config.instance.segment_size, @replicator)
      drop_overflow
    end

    def close : Nil
      super
      @expiry_changed.close
      @consumer_offsets.close
    end

    def delete
      super
      @consumer_offsets.delete
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

    # Resolves `offset` to the {offset, segment, position} to start reading at
    def find_offset(offset : StreamOffset::Any) : Tuple(Int64, UInt32, UInt32)
      raise ClosedError.new if @closed
      case offset
      in StreamOffset::First     then offset_at(@segments.first_key, 4u32)
      in StreamOffset::Last      then offset_at(@segments.last_key, 4u32)
      in StreamOffset::Next      then last_offset_seg_pos
      in StreamOffset::Timestamp then find_offset_in_segments(offset.time)
      in StreamOffset::FromEnd   then find_offset_from_end(offset.count)
      in StreamOffset::Absolute
        offset.value > @last_offset ? last_offset_seg_pos : find_offset_in_segments(offset.value)
      end
    end

    def acquire_segment(consumer : StreamConsumer) : Nil
      return if consumer.segment_acquired?
      consumer.segment_acquired = true
      seg = consumer.segment
      @segment_readers[seg] = (@segment_readers[seg]? || 0u32) + 1
    end

    # Readahead for a fast consumer reading a full segment, segments are
    # otherwise mapped without it (see MessageStore#open_segment).
    # Normal rather than sequential advice: several consumers can read the
    # same segment, and the kernel evicts pages read through a sequential
    # mapping early, possibly before the next consumer has read them.
    private def read_ahead(mfile : MFile) : Nil
      mfile.advise(MFile::Advice::Normal) unless mfile == @wfile
    end

    def release_segment(consumer : StreamConsumer) : Nil
      return unless consumer.segment_acquired?
      consumer.segment_acquired = false
      release_segment(consumer.segment)
    end

    private def release_segment(seg : UInt32) : Nil
      count = @segment_readers[seg]? || return
      if count > 1
        @segment_readers[seg] = count - 1
      else
        @segment_readers.delete(seg)
        unmap_if_unused(seg)
      end
    end

    # Drops a segment's pages from memory once no consumer is reading from it.
    # The active write segment is kept mapped.
    def unmap_if_unused(seg : UInt32) : Nil
      return if @closed
      return if @segment_readers.has_key?(seg)
      mfile = @segments[seg]? || return
      return if mfile == @wfile
      mfile.dontneed
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

    private def find_offset_from_end(count : Int64) : Tuple(Int64, UInt32, UInt32)
      return last_offset_seg_pos if @size.zero?

      first_offset, _seg, _pos = offset_at(@segments.first_key, 4u32)
      target_offset = @last_offset - count + 1
      target_offset = first_offset if target_offset < first_offset
      find_offset_in_segments(target_offset)
    end

    private def find_offset_in_segments(offset : Int | Time) : Tuple(Int64, UInt32, UInt32)
      segment = offset_index_lookup(offset)
      pos = 4u32
      msg_offset = @segment_first_offset[segment] || 0i64
      @segments[segment]?.try { |mfile| read_ahead(mfile) }
      loop do
        rfile = @segments[segment]?
        if rfile.nil? || pos == rfile.size
          unmap_if_unused(segment)
          if segment = @segments.each_key.find { |sid| sid > segment }
            rfile = @segments[segment]
            read_ahead(rfile)
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
      @consumer_offsets.last_offset_by_tag(consumer_tag)
    end

    def store_consumer_offset(consumer_tag : String, new_offset : Int64)
      raise ClosedError.new if @closed
      @consumer_offsets.store(consumer_tag, new_offset) { lowest_offset_in_stream }
    end

    def cleanup_consumer_offsets
      @consumer_offsets.cleanup { lowest_offset_in_stream }
    end

    # Lowest offset still retained in the stream, used to discard consumer
    # offsets that have fallen out of the stream during cleanup.
    private def lowest_offset_in_stream : Int64
      offset_at(@segments.first_key, 4u32).first
    end

    # Like #read, but yields the message outside `lock`, see #shift_with_lease?
    def read_with_lease?(lock : Mutex, segment : UInt32, position : UInt32, & : Envelope -> _) : Bool
      env = lock.synchronize { read(segment, position).try &.lease } || return false
      begin
        yield env
      ensure
        env.release
      end
      true
    end

    def read(segment : UInt32, position : UInt32) : Envelope?
      return if @closed
      rfile = @segments[segment]? || return # dropped by retention
      return if position == rfile.size
      begin
        msg = BytesMessage.from_bytes(rfile.to_slice + position)
        sp = SegmentPosition.new(segment, position, msg.bytesize.to_u32)
        Envelope.new(sp, msg, redelivered: false, segment: rfile)
      rescue ex
        puts "read segment=#{segment} position=#{position}"
        raise Error.new(rfile, cause: ex)
      end
    end

    def shift?(consumer : AMQP::StreamConsumer) : Envelope?
      raise ClosedError.new if @closed

      if env = shift_requeued(consumer)
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
        Envelope.new(sp, msg, redelivered: false, segment: rfile)
      rescue ex
        raise Error.new(rfile, cause: ex)
      end
    end

    private def shift_requeued(consumer) : Envelope?
      while sp = consumer.requeued.shift?
        if segment = @segments[sp.segment]? # segment might have expired since requeued
          begin
            msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
            offset, _, _ = offset_at(sp.segment, sp.position)
            unmap_if_unused(sp.segment) if consumer.requeued.none? { |r| r.segment == sp.segment }
            msg.properties.headers = add_offset_header(msg.properties.headers, offset)
            return Envelope.new(sp, msg, redelivered: true, segment: segment)
          rescue ex
            raise Error.new(segment, cause: ex)
          end
        end
      end
    end

    def next_segment_id(segment) : UInt32?
      @segments.each_key.find { |sid| sid > segment }
    end

    # The segment after `segment` and the offset of its first message
    def next_segment_offset(segment) : Tuple(UInt32, Int64)?
      if seg = next_segment_id(segment)
        {seg, @segment_first_offset[seg]}
      end
    end

    private def next_segment(consumer) : MFile?
      if seg_id = next_segment_id(consumer.segment)
        fast = @segments[consumer.segment]?.try { |prev| read_fast?(prev, consumer.segment_since) }
        release_segment(consumer)
        consumer.segment = seg_id
        consumer.pos = 4u32
        consumer.segment_since = RoughTime.instant
        acquire_segment(consumer)
        @segments[seg_id].tap { |mfile| read_ahead(mfile) if fast }
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

    # Streams don't use the inherited @rfile, so unmap unless a consumer is reading it
    private def unmap_finished_segment(seg : UInt32, mfile : MFile) : Nil
      mfile.dontneed unless @segment_readers.has_key?(seg)
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      super.tap do
        @expiry_changed.try_send?(nil)
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
      drop_overflow_by_length
      drop_overflow_by_age
      cleanup_consumer_offsets
    end

    def drop_expired : Nil
      return if @closed
      cleanup_consumer_offsets if drop_overflow_by_age
    end

    # When the oldest segment expires, nil if no segment can be dropped
    # (no max-age, or only the write segment is left)
    def next_expiry : Time?
      max_age = @max_age || return
      @segments.each do |seg_id, mfile|
        return if mfile == @wfile
        return Time.unix_ms(@segment_last_ts[seg_id]) + max_age
      end
    end

    # Only drops a segment if what remains still meets the limit, so the
    # stream always keeps at least max-length messages/max-length-bytes bytes
    private def drop_overflow_by_length : Bool
      dropped = false
      if max_length = @max_length
        dropped |= drop_segments_while do |seg_id|
          @size.to_i64 - @segment_msg_count[seg_id] >= max_length
        end
      end
      if max_bytes = @max_length_bytes
        dropped |= drop_segments_while do |seg_id|
          @bytesize.to_i64 - (@segments[seg_id].size - 4) >= max_bytes
        end
      end
      dropped
    end

    private def drop_overflow_by_age : Bool
      max_age = @max_age || return false
      min_ts = RoughTime.utc - max_age
      drop_segments_while do |seg_id|
        Time.unix_ms(@segment_last_ts[seg_id]) < min_ts
      end
    end

    # Returns true if any segment was dropped
    private def drop_segments_while(& : UInt32 -> Bool) : Bool
      size_before = @segments.size
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
      @segments.size != size_before
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

    # Streams never ack individual messages, so any ack files are leftovers
    private def load_acks_from_disk : Nil
      return if @closed
      Dir.each_child(@msg_dir) do |f|
        next unless f.starts_with?("acks.") || f.starts_with?("tmp.acks.")
        path = File.join(@msg_dir, f)
        @log.info { "Deleting ack file not used by streams: #{path}" }
        File.delete?(path)
        @replicator.try &.delete_file(path)
      end
    rescue File::NotFoundError
      # msg_dir does not exist, nothing to load
    end

    private def prune_orphaned_acks : Nil
    end

    private def delete_unused_segments : Nil
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
  end
end
