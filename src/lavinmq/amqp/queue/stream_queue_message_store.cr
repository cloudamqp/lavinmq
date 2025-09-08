require "./stream_queue"
require "../stream_consumer"
require "../../store/offset"

module LavinMQ::AMQP
  class StreamMessageStore < MessageStore
    getter new_messages = ::Channel(Bool).new
    getter offsets : OffsetStore
    property max_length : Int64?
    property max_length_bytes : Int64?
    property max_age : Time::Span | Time::MonthSpan | Nil
    @segment_last_ts = Hash(UInt32, Int64).new(0i64) # used for max-age

    def initialize(@msg_dir : String, @replicator : Clustering::Replicator?, @offsets : OffsetStore, durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
      super
      # @log = Logger.new(Log, metadata)
      # @durable = durable
      # @acks = Hash(UInt32, MFile).new { |acks, seg| acks[seg] = open_ack_file(seg) }
      # load_segments_from_disk
      # delete_orphan_ack_files
      # load_deleted_from_disk
      # delete_unused_segments
      # load_stats_from_segments
      # @wfile_id = @segments.last_key
      # @wfile = @segments.last_value
      # @rfile_id = @segments.first_key
      # @rfile = @segments.first_value
      # @empty.set empty?
      # drop_overflow
    end

    def close : Nil
      super
      # @offset_file.close
    end

    def delete
      super
      # delete_file(@offset_file)
    end

    def unmap_segments(except : Enumerable(UInt32) = StaticArray(UInt32, 0).new(0u32))
      @segments.each do |seg_id, mfile|
        next if mfile == @wfile
        next if except.includes? seg_id
        mfile.dontneed
      end
    end

    def shift?(consumer : AMQP::StreamConsumer) : Envelope?
      raise ClosedError.new if @closed

      if env = shift_requeued(consumer.requeued)
        return env
      end

      return if consumer.offset > @offsets.last_offset
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
      msg.properties.headers = if headers = msg.properties.headers
                                 headers["x-stream-offset"] = @offsets.next_offset
                                 headers
                               else
                                 AMQP::Table.new({"x-stream-offset": @offsets.next_offset})
                               end
      sp = write_to_disk(msg)
      @bytesize += sp.bytesize
      @size += 1
      @segment_last_ts[sp.segment] = msg.timestamp
      # Consumer notification is handled by StreamQueue.publish directly
      # to ensure all consumers are notified,
      # not just one via the shared channel like other queue types
      sp
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      super.tap do
        drop_overflow
        @offsets.from_metadata(@segments.last_key, @offsets.last_offset + 1, RoughTime.unix_ms)
      end
    end

    private def write_metadata(io, seg)
      super
      @offsets.write_metadata(io, seg)
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
      @offsets.cleanup_consumer_offsets
    end

    private def drop_segments_while(& : UInt32 -> Bool)
      @segments.reject! do |seg_id, mfile|
        should_drop = yield seg_id
        break unless should_drop
        next if mfile == @wfile # never delete the last active segment
        msg_count = @segment_msg_count.delete(seg_id)
        @size -= msg_count if msg_count
        @segment_last_ts.delete(seg_id)
        @offsets.drop_segment(seg_id)
        @bytesize -= mfile.size - 4
        delete_file(mfile)
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
    end

    private def offset_from_headers(headers) : Int64
      headers.not_nil!("Message lacks headers")["x-stream-offset"].as(Int64)
    end

    private def produce_metadata(seg, mfile)
      super

      @offsets.produce_metadata(seg, mfile)
    end

    private def read_metadata_file(seg, mfile)
      File.open("#{mfile.path}.meta") do |file|
        count = file.read_bytes(UInt32)
        offset_index = file.read_bytes(Int64)
        timestamp_index = file.read_bytes(Int64)
        @offsets.from_metadata(seg, offset_index, timestamp_index)
        @segment_msg_count[seg] = count
        bytesize = mfile.size - 4
        if deleted = @deleted[seg]?
          deleted.each do |pos|
            mfile.pos = pos
            bytesize -= BytesMessage.skip(mfile)
            count -= 1
          end
        end
        mfile.pos = 4
        mfile.dontneed
        @bytesize += bytesize
        @size += count
        @log.debug { "Reading count from #{mfile.path}.meta: #{count}" }
      end
    end
  end
end
