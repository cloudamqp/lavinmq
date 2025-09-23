require "./mfile"
require "./segment_position"
require "log"
require "file_utils"
require "./clustering/server"
require "./bool_channel"
require "./store/message"
require "./store/message_metadata"
require "./store/message_segments"

module LavinMQ
  # Message store
  # This handles writing msgs to segments on disk
  # Keeping a list of deleted messages in memory and on disk
  # You can shift through the message store, but not requeue msgs
  # That has to be handled at another layer
  # Writes messages to segments on disk
  # Messages are refered to as SegmentPositions
  # Deleted messages are written to acks.#{segment}
  class QueueMessageStore
    include MessageStore
    include MessageStoreMetadata
    include MessageStoreSegments
    PURGE_YIELD_INTERVAL = 16_384
    Log                  = LavinMQ::Log.for "message_store"
    getter segments = Hash(UInt32, MFile).new
    @deleted = Hash(UInt32, Array(UInt32)).new
    @segment_msg_count = Hash(UInt32, UInt32).new(0u32)
    @requeued = Deque(SegmentPosition).new
    @closed = false
    getter closed
    getter bytesize = 0u64
    getter size = 0u32
    getter empty = BoolChannel.new(true)
    getter msg_dir

    def initialize(@msg_dir : String, @replicator : Clustering::Replicator?, durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
      @log = Logger.new(Log, metadata)
      @durable = durable
      @acks = Hash(UInt32, MFile).new { |acks, seg| acks[seg] = open_ack_file(seg) }
      load_segments_from_disk
      delete_orphan_ack_files
      load_deleted_from_disk
      load_stats_from_segments
      delete_unused_segments
      @wfile_id = @segments.last_key
      @wfile = @segments.last_value
      @rfile_id = @segments.first_key
      @rfile = @segments.first_value
      @empty.set empty?
    end

    def push(msg) : SegmentPosition
      raise ClosedError.new if @closed
      sp = write_to_disk(msg)
      was_empty = @size.zero?
      @bytesize += sp.bytesize
      @size += 1
      @empty.set false if was_empty
      sp
    end

    def requeue(sp : SegmentPosition)
      raise ClosedError.new if @closed
      if idx = @requeued.bsearch_index { |rsp| rsp > sp }
        @requeued.insert(idx, sp)
      else
        @requeued.push(sp)
      end
      was_empty = @size.zero?
      @bytesize += sp.bytesize
      @size += 1
      @empty.set false if was_empty
    end

    def first? : Envelope? # ameba:disable Metrics/CyclomaticComplexity
      raise ClosedError.new if @closed
      if sp = @requeued.first?
        seg = @segments[sp.segment]
        begin
          msg = BytesMessage.from_bytes(seg.to_slice + sp.position)
          return Envelope.new(sp, msg, redelivered: true)
        rescue ex
          raise Error.new(seg, cause: ex)
        end
      end

      loop do
        seg = @rfile_id
        rfile = @rfile
        pos = rfile.pos.to_u32
        if pos == rfile.size # EOF?
          select_next_read_segment && next
          return if @size.zero?
          raise IO::EOFError.new("EOF but @size=#{@size}")
        end
        if deleted?(seg, pos)
          BytesMessage.skip(rfile)
          next
        end
        msg = BytesMessage.from_bytes(rfile.to_slice + pos)
        sp = SegmentPosition.make(seg, pos, msg)
        return Envelope.new(sp, msg, redelivered: false)
      rescue ex : IndexError
        @log.warn(exception: ex) { "Msg file size does not match expected value, moving on to next segment" }
        select_next_read_segment && next
        return if @size.zero?
        raise Error.new(@rfile, cause: ex)
      rescue ex
        raise Error.new(@rfile, cause: ex)
      end
    end

    def shift?(consumer = nil) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
      raise ClosedError.new if @closed
      if sp = @requeued.shift?
        segment = @segments[sp.segment]
        begin
          msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
          @bytesize -= sp.bytesize
          @size -= 1
          @empty.set true if @size.zero?
          return Envelope.new(sp, msg, redelivered: true)
        rescue ex
          raise Error.new(segment, cause: ex)
        end
      end

      loop do
        rfile = @rfile
        seg = @rfile_id
        pos = rfile.pos.to_u32
        if pos == rfile.size # EOF?
          select_next_read_segment && next
          return if @size.zero?
          raise IO::EOFError.new("EOF but @size=#{@size}")
        end
        if deleted?(seg, pos)
          BytesMessage.skip(rfile)
          next
        end
        msg = BytesMessage.from_bytes(rfile.to_slice + pos)
        sp = SegmentPosition.make(seg, pos, msg)
        rfile.seek(sp.bytesize, IO::Seek::Current)
        @bytesize -= sp.bytesize
        @size -= 1
        @empty.set true if @size.zero?
        return Envelope.new(sp, msg, redelivered: false)
      rescue ex : IndexError
        @log.warn(exception: ex) { "Msg file size does not match expected value, moving on to next segment" }
        select_next_read_segment && next
        return if @size.zero?
        raise Error.new(@rfile, cause: ex)
      rescue ex
        raise Error.new(@rfile, cause: ex)
      end
    end

    def [](sp : SegmentPosition) : BytesMessage
      raise ClosedError.new if @closed
      segment = @segments[sp.segment]
      begin
        BytesMessage.from_bytes(segment.to_slice + sp.position)
      rescue ex
        raise Error.new(segment, cause: ex)
      end
    end

    def delete(sp) : Nil
      raise ClosedError.new if @closed
      afile = @acks[sp.segment]
      begin
        afile.write_bytes sp.position
        @replicator.try &.append(afile.path, sp.position)

        # if all msgs in a segment are deleted then delete the segment
        return if sp.segment == @wfile_id # don't try to delete a segment we still write to
        ack_count = afile.size // sizeof(UInt32)
        msg_count = @segment_msg_count[sp.segment]
        if ack_count == msg_count
          @log.debug { "Deleting segment #{sp.segment}" }
          select_next_read_segment if sp.segment == @rfile_id
          if a = @acks.delete(sp.segment)
            delete_file(a)
          end
          if seg = @segments.delete(sp.segment)
            delete_file(seg, including_meta: true)
          end
          @segment_msg_count.delete(sp.segment)
          @deleted.delete(sp.segment)
        end
      rescue ex
        raise Error.new(afile, cause: ex)
      end
    end

    # Deletes all "ready" messages (not unacked)
    def purge(max_count : Int = UInt32::MAX) : UInt32
      raise ClosedError.new if @closed
      count = 0u32
      while count < max_count && (env = shift?)
        delete(env.segment_position)
        count += 1
        break if count >= max_count
        Fiber.yield if (count % PURGE_YIELD_INTERVAL).zero?
      end
      count
    end

    def purge_all
      # Delete all segments except the current rfile and wfile
      @segments.reject! do |seg_id, file|
        next false if seg_id == @rfile_id || seg_id == @wfile_id

        delete_file(file, including_meta: true)
        if msg_count = @segment_msg_count.delete(seg_id)
          @size -= msg_count
        end
        if afile = @acks.delete(seg_id)
          delete_file(afile)
        end
        @deleted.delete(seg_id)
        true
      end

      # Purge @rfile and @wfile
      while env = shift?
        delete(env.segment_position)
      end

      @requeued = Deque(SegmentPosition).new
      @bytesize = 0_u64
    end

    def avg_bytesize : UInt32
      return 0u32 if @size.zero?
      (@bytesize / @size).to_u32
    end
  end
end
