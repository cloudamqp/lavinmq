require "../clustering/replicator"
require "../amqp/stream/message_store"

module LavinMQ
  record Offset, offset : Int64, segment : UInt32, position : UInt32

  class OffsetStore
    getter last_offset : Int64
    @consumer_offset_positions = Hash(String, Int64).new # used for consumer offsets
    @offset_index = Hash(UInt32, Int64).new              # segment_id => offset of first msg
    @timestamp_index = Hash(UInt32, Int64).new           # segment_id => ts of first msg

    def initialize(@store : AMQP::StreamMessageStore, @replicator : Clustering::Replicator? = nil)
      p = File.join(@store.msg_dir, "consumer_offsets")
      @consumer_offsets = MFile.new(p, Config.instance.segment_size)
      @replicator.try &.register_file @consumer_offsets
      @last_offset = get_last_offset
      @consumer_offset_positions = restore_consumer_offset_positions
    end

    def close : Nil
      @consumer_offsets.close
    end

    def next_offset
      @last_offset += 1
    end

    private def get_last_offset : Int64
      return 0i64 if @store.empty?
      bytesize = 0_u32
      _, mfile = @store.last_segment
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
    # ameba:disable Metrics/CyclomaticComplexity
    def find_offset(offset, tag = nil, track_offset = false) : Offset
      if track_offset
        consumer_last_offset = last_offset_by_consumer_tag(tag)
        return find_offset_in_segments(consumer_last_offset) if consumer_last_offset
      end

      case offset
      when "first" then offset_at(@store.first_segment[0], 4u32)
      when "last"  then offset_at(@store.last_segment[0], 4u32)
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

    private def offset_at(seg, pos, retried = false) : Offset
      return Offset.new(@last_offset, seg, pos) if @store.empty?

      mfile = @store.segment(seg)
      unless mfile
        return offset_at(seg + 1, 4_u32, true)
      end
      msg = BytesMessage.from_bytes(mfile.to_slice + pos)
      offset = offset_from_headers(msg.properties.headers)
      Offset.new(offset, seg, pos)
      # rescue ex : IndexError # first segment can be empty if message size >= segment size
      #   return offset_at(seg + 1, 4_u32, true) unless retried
      #   raise ex
    end

    private def offset_from_headers(headers) : Int64
      headers.not_nil!("Message lacks headers")["x-stream-offset"].as(Int64)
    end

    private def last_offset_seg_pos
      seg = @store.last_segment
      Offset.new(@last_offset, seg[0], seg[1].size.to_u32)
    end

    private def find_offset_in_segments(offset : Int | Time) : Offset
      segment = offset_index_lookup(offset)
      pos = 4u32
      msg_offset = 0i64
      loop do
        rfile = @store.segment segment
        if rfile.nil? || pos == rfile.size
          seg = @store.each_segment.find { |v| v[0] > segment }
          if v = seg
            rfile = v[1]
            pos = 4_u32
          end
        end
        return last_offset_seg_pos unless rfile
        msg = BytesMessage.from_bytes(rfile.to_slice + pos)
        msg_offset = offset_from_headers(msg.properties.headers)
        case offset
        in Int  then break if offset <= msg_offset
        in Time then break if offset <= Time.unix_ms(msg.timestamp)
        end
        pos += msg.bytesize
      end
      Offset.new(msg_offset, segment, pos)
    end

    private def offset_index_lookup(offset) : UInt32
      seg, _ = @store.first_segment
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
      cleanup_consumer_offsets if consumer_offset_file_full?(consumer_tag)
      start_pos = @consumer_offsets.size
      @consumer_offsets.write_bytes AMQ::Protocol::ShortString.new(consumer_tag)
      @consumer_offset_positions[consumer_tag] = @consumer_offsets.size
      @consumer_offsets.write_bytes new_offset
      len = 1 + consumer_tag.bytesize + 8
      @replicator.try &.append(@consumer_offsets.path, @consumer_offsets.to_slice(start_pos, len))
    end

    def consumer_offset_file_full?(consumer_tag)
      (@consumer_offsets.size + 1 + consumer_tag.bytesize + 8) >= @consumer_offsets.capacity
    end

    def cleanup_consumer_offsets
      return if @consumer_offsets.size.zero?
      offsets_to_save = Hash(String, Int64).new
      s = @store.first_segment
      lowest_offset_in_stream = offset_at(s[0], 4u32)
      capacity = 0
      @consumer_offset_positions.each do |ctag, _pos|
        if offset = last_offset_by_consumer_tag(ctag)
          offsets_to_save[ctag] = offset if offset >= lowest_offset_in_stream.offset
          capacity += ctag.bytesize + 1 + 8
        end
      end
      @consumer_offset_positions = Hash(String, Int64).new
      replace_offsets_file(capacity * 1000) do
        offsets_to_save.each do |ctag, offset|
          store_consumer_offset(ctag, offset)
        end
      end
    end

    def replace_offsets_file(capacity : Int, &)
      deletion_replicated = WaitGroup.new
      @replicator.try &.delete_file(@consumer_offsets.path, deletion_replicated) # FIXME: this is not entirely safe, but replace_file is worse
      old_consumer_offsets = @consumer_offsets
      @consumer_offsets = MFile.new("#{old_consumer_offsets.path}.tmp", capacity)
      yield # fill the new file with correct data in this block
      @consumer_offsets.rename(old_consumer_offsets.path)
      spawn(name: "wait for consumeroffset deletion to be replicated") do
        deletion_replicated.wait
        old_consumer_offsets.close(truncate_to_size: false)
      end
    end

    def drop_segment(seg_id)
      @offset_index.delete(seg_id)
      @timestamp_index.delete(seg_id)
    end

    def write_metadata(io, seg)
      io.write_bytes @offset_index[seg]
      io.write_bytes @timestamp_index[seg]
    end

    def produce_metadata(seg, mfile)
      msg = BytesMessage.from_bytes(mfile.to_slice + 4u32)
      @offset_index[seg] = offset_from_headers(msg.properties.headers) # TODO
      @timestamp_index[seg] = msg.timestamp
    rescue IndexError
      from_metadata(seg, @last_offset, RoughTime.unix_ms)
    end

    def from_metadata(seg, offset, timestamp)
      @offset_index[seg] = offset
      @timestamp_index[seg] = timestamp
    end
  end
end
