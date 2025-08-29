require "./mfile"
require "./segment_position"
require "log"
require "file_utils"
require "./clustering/server"
require "./bool_channel"

module LavinMQ
  # Message store
  # This handles writing msgs to segments on disk
  # Keeping a list of deleted messages in memory and on disk
  # You can shift through the message store, but not requeue msgs
  # That has to be handled at another layer
  # Writes messages to segments on disk
  # Messages are refered to as SegmentPositions
  # Deleted messages are written to acks.#{segment}
  class MessageStore
    include LavinMQ::Logging::Loggable
    PURGE_YIELD_INTERVAL = 16_384
    Log                  = LavinMQ::Log.for "message_store"
    @segments = Hash(UInt32, MFile).new
    @deleted = Hash(UInt32, Array(UInt32)).new
    @segment_msg_count = Hash(UInt32, UInt32).new(0u32)
    @requeued = Deque(SegmentPosition).new
    @closed = false
    getter closed
    getter bytesize = 0u64
    getter size = 0u32
    getter empty = BoolChannel.new(true)

    def initialize(@msg_dir : String, @replicator : Clustering::Replicator?, durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
      L.context(metadata)
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
        raise IndexError.new("Message at segment #{seg} pos #{pos} has zero timestamp") if msg.timestamp.zero?
        sp = SegmentPosition.make(seg, pos, msg)
        return Envelope.new(sp, msg, redelivered: false)
      rescue ex : IndexError
        L.warn "Msg file size does not match expected value, moving on to next segment", exception: ex
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
        raise IndexError.new("Message at segment #{seg} pos #{pos} has zero timestamp") if msg.timestamp.zero?
        sp = SegmentPosition.make(seg, pos, msg)
        rfile.seek(sp.bytesize, IO::Seek::Current)
        @bytesize -= sp.bytesize
        @size -= 1
        @empty.set true if @size.zero?
        return Envelope.new(sp, msg, redelivered: false)
      rescue ex : IndexError
        L.warn "Msg file size does not match expected value, moving on to next segment", exception: ex
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
          L.debug "Deleting segment", segment: sp.segment
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

    def delete
      @closed = true
      @empty.close
      @segments.reject! { |_, f| delete_file(f, including_meta: true); true }
      @acks.reject! { |_, f| delete_file(f); true }
      FileUtils.rm_rf @msg_dir
    end

    private def delete_file(file : MFile, including_meta = false)
      File.delete?(meta_file_name(file)) if including_meta
      file.delete(raise_on_missing: false)
      if replicator = @replicator
        replicator.delete_file(meta_file_name(file), WaitGroup.new) if including_meta
        wg = WaitGroup.new
        replicator.delete_file(file.path, wg)
        spawn(name: "wait for file deletion is replicated") do
          wg.wait
          file.close
        end
      else
        file.close
      end
    end

    def empty?
      @size.zero?
    end

    def close : Nil
      return if @closed
      @closed = true
      delete_orphan_ack_files
      @empty.close
      # To make sure that all replication actions for the segments
      # have finished wait for a delete action of a nonexistent file
      if replicator = @replicator
        wg = WaitGroup.new
        replicator.delete_file(File.join(@msg_dir, "nonexistent"), wg)
        spawn(name: "wait for file deletion is replicated") do
          wg.wait
          @segments.each_value &.close
          @acks.each_value &.close
        end
      else
        @segments.each_value &.close
        @acks.each_value &.close
      end
    end

    def avg_bytesize : UInt32
      return 0u32 if @size.zero?
      (@bytesize / @size).to_u32
    end

    private def select_next_read_segment : MFile?
      @rfile.dontneed unless @rfile.closed?
      # Expect @segments to be ordered
      if id = @segments.each_key.find { |sid| sid > @rfile_id }
        rfile = @segments[id]
        rfile.advise(MFile::Advice::Sequential)
        @rfile_id = id
        @rfile = rfile
      end
    end

    private def write_to_disk(msg) : SegmentPosition
      wfile = @wfile
      if wfile.capacity < wfile.size + msg.bytesize
        wfile = open_new_segment(msg.bytesize)
      end
      wfile_id = @wfile_id
      sp = SegmentPosition.make(wfile_id, wfile.size.to_u32, msg)
      wfile.write_bytes msg
      @replicator.try &.append(wfile.path, wfile.to_slice(sp.position, wfile.size - sp.position))
      @segment_msg_count[wfile_id] += 1
      sp
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      unless @wfile_id.zero?
        write_metadata_file(@wfile_id, @wfile)
        @wfile.truncate(@wfile.size)
      end
      @wfile.dontneed unless @wfile == @rfile
      next_id = @wfile_id + 1
      path = File.join(@msg_dir, "msgs.#{next_id.to_s.rjust(10, '0')}")
      capacity = Math.max(Config.instance.segment_size, next_msg_size + 4)
      wfile = MFile.new(path, capacity)
      wfile.write_bytes Schema::VERSION
      wfile.pos = 4
      @replicator.try &.register_file wfile
      @replicator.try &.append path, Schema::VERSION
      @wfile_id = next_id
      @wfile = @segments[next_id] = wfile
      delete_unused_segments
      @wfile.delete unless @durable # mark as deleted if non-durable
      wfile
    end

    private def write_metadata_file(seg : UInt32, wfile : MFile)
      metafile = meta_file_name(wfile)
      L.debug "Write message segment meta file #{metafile}"
      File.open(metafile, "w") do |f|
        f.buffer_size = 4096
        write_metadata(f, seg)
        @replicator.try &.register_file(f)
      end
      @replicator.try &.replace_file metafile
    end

    private def write_metadata(io, seg)
      io.write_bytes @segment_msg_count[seg]
    end

    private def open_ack_file(id) : MFile
      path = File.join(@msg_dir, "acks.#{id.to_s.rjust(10, '0')}")
      capacity = Config.instance.segment_size // BytesMessage::MIN_BYTESIZE * 4 + 4
      mfile = MFile.new(path, capacity, writeonly: true)
      mfile.delete unless @durable # mark as deleted if non-durable
      @replicator.try &.register_file mfile
      mfile
    end

    private def load_deleted_from_disk
      count = 0u32
      ack_files = 0u32
      Dir.each(@msg_dir) do |f|
        ack_files += 1 if f.starts_with? "acks."
      end

      L.debug "Loading ack files", count: ack_files
      Dir.each_child(@msg_dir) do |child|
        next unless child.starts_with? "acks."
        seg = child[5, 10].to_u32
        acked = Array(UInt32).new
        File.open(File.join(@msg_dir, child), "a+") do |file|
          loop do
            pos = UInt32.from_io(file, IO::ByteFormat::SystemEndian)
            if pos.zero? # pos 0 doesn't exists (first valid is 4), must be a sparse file
              file.truncate(file.pos - 4)
              break
            end
            acked << pos
          rescue IO::EOFError
            break
          end
          @replicator.try &.register_file(file)
        end
        @acks[seg] = open_ack_file(seg)
        L.debug "Loaded ack files", loaded: count + 1, total: ack_files if (count += 1) % 128 == 0
        @deleted[seg] = acked.sort! unless acked.empty?
        Fiber.yield
      end
      L.debug "Loaded ack files", count: count
    end

    private def load_segments_from_disk : Nil
      ids = Array(UInt32).new
      Dir.each_child(@msg_dir) do |f|
        if f.starts_with?("msgs.") && f.size == 15
          ids << f[5, 10].to_u32
        end
      end
      ids.sort!
      was_empty = ids.empty?
      ids << 1_u32 if was_empty
      last_idx = ids.size - 1
      ids.each_with_index do |seg, idx|
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        path = File.join(@msg_dir, filename)
        file = if idx == last_idx
                 # expand the last segment
                 MFile.new(path, Config.instance.segment_size)
               else
                 MFile.new(path)
               end
        @replicator.try &.register_file file
        file.delete unless @durable # mark files for non-durable queues for deletion

        if was_empty
          file.write_bytes Schema::VERSION
          @replicator.try &.append path, Schema::VERSION
        else
          begin
            SchemaVersion.verify(file, :message)
          rescue IO::EOFError
            # delete empty file, it will be recreated if it's needed
            L.warn "Empty file, deleting it", path: path
            delete_file(file, including_meta: true)
            if idx == 0 # Recreate the file if it's the first segment because we need at least one segment to exist
              file = MFile.new(path, Config.instance.segment_size)
              file.write_bytes Schema::VERSION
              @replicator.try &.append path, Schema::VERSION
            else
              @segments.delete seg
              next
            end
          rescue ex
            L.error "Could not initialize segment, closing message store", segment: seg, exception: ex
            close
          end
        end
        file.pos = 4
        @segments[seg] = file
        Fiber.yield
      end
    end

    # Populate bytesize, size and segment_msg_count
    private def load_stats_from_segments : Nil
      counter = 0
      is_long_queue = @segments.size > 255
      if is_long_queue
        L.info "Loading segments", count: @segments.size
      else
        L.debug "Loading segments", count: @segments.size
      end
      @segments.each do |seg, mfile|
        begin
          read_metadata_file(seg, mfile)
        rescue File::NotFoundError | MetadataError
          produce_metadata(seg, mfile)
          write_metadata_file(seg, mfile) unless seg == @segments.last_key # this segment is not full yet
        end

        if is_long_queue
          L.info "Loaded segments", loaded: counter &+= 1, total: @segments.size, messages: @size if counter % 128 == 0
        else
          L.debug "Loaded segments", loaded: counter &+= 1, total: @segments.size, messages: @size if counter % 128 == 0
        end
      end
      L.info "Loaded segments", count: counter, messages: @size
    end

    private def read_metadata_file(seg, mfile)
      metafile = meta_file_name(mfile)
      count = File.open(metafile) do |file|
        msg_count = file.read_bytes(UInt32)
        @replicator.try &.register_file(file)
        read_extra_metadata_fields(file, seg)
        msg_count
      end
      @segment_msg_count[seg] = count
      bytesize = mfile.size - 4
      if deleted = @deleted[seg]?
        deleted.each do |pos|
          mfile.pos = pos
          bs = BytesMessage.skip(mfile)
          # don't allow underflow
          bytesize = bs < bytesize ? bytesize - bs : 0
          count -= 1 if count > 0
        rescue ex
          @log.error { "Error reading metadata file #{metafile}, pos: #{pos}, seg: #{seg}, count: #{count}, bytesize: #{bytesize}" }
          raise ex
        end
      end
      mfile.pos = 4
      mfile.dontneed
      @bytesize += bytesize
      @size += count
      L.debug "Reading count from #{metafile}: #{count}"
    rescue ex : File::NotFoundError
      raise ex
    rescue ex
      L.error "Metadata file #{metafile} is incorrect", exception: ex
      raise MetadataError.new("Metadata file #{metafile} is incorrect", cause: ex)
    end

    private def read_extra_metadata_fields(file : File, seg : UInt32)
      # Used in subclasses of MessageStore to read additional metadata fields
    end

    private def produce_metadata(seg, mfile)
      count = 0u32
      mfile.pos = 4
      loop do
        pos = mfile.pos
        ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
        break mfile.resize(pos) if ts.zero? # This means that the rest of the file is zero, so resize it
        bytesize = BytesMessage.skip(mfile)
        count += 1
        next if deleted?(seg, pos)
        @bytesize += bytesize
        @size += 1
      rescue ex : IO::EOFError
        break
      rescue ex : OverflowError | AMQ::Protocol::Error::FrameDecode
        L.error "Could not initialize segment, closing message store", segment: seg, position: mfile.pos, exception: ex
        close
        return count
      end
      mfile.pos = 4
      mfile.dontneed
      Fiber.yield
      @segment_msg_count[seg] = count
      L.debug "Manually counted messages", count: count, path: mfile.path
    end

    private def delete_unused_segments : Nil
      current_seg = @segments.last_key
      @segments.reject! do |seg, mfile|
        next if seg == current_seg # don't the delete the segment still being written to

        if (acks = @acks[seg]?) && @segment_msg_count[seg] <= (acks.size // sizeof(UInt32))
          L.debug "Deleting unused segment #{seg}"
          @segment_msg_count.delete seg
          @deleted.delete seg
          if ack = @acks.delete(seg)
            delete_file(ack)
          end
          delete_file(mfile, including_meta: true)
          true
        else
          false
        end
      end
    end

    private def delete_orphan_ack_files
      Dir.each_child(@msg_dir) do |f|
        next unless f.starts_with? "acks."
        seg = f[5, 10].to_u32
        unless @segments.has_key?(seg)
          path = File.join(@msg_dir, f)
          L.warn "Deleting orphaned ack file", path: path
          File.delete(path)
          @replicator.try &.delete_file(path, WaitGroup.new)
        end
      end
    end

    private def deleted?(seg, pos) : Bool
      if del = @deleted[seg]?
        del.bsearch { |dpos| dpos >= pos } == pos
      else
        false
      end
    end

    private def meta_file_name(mfile : MFile) : String
      meta_file_name(mfile.path)
    end

    private def meta_file_name(path : String) : String
      # We assume the path ends with "msgs.<10 chars>"
      raw = path.to_slice

      # This is basically the same as using sub("msgs.", "meta.") but with #sub
      # the first occurrence of "msgs." would be replaced, not the last one.
      # This also requires only one allocation.
      String.build(path.size) do |io|
        io.write raw[0, raw.size - 15]
        io.write "meta.".to_slice
        io.write raw[-10..]
      end
    end

    class ClosedError < ::Channel::ClosedError; end

    class MetadataError < Exception; end

    class Error < Exception
      def initialize(mfile : MFile, cause = nil)
        super("path=#{mfile.path} pos=#{mfile.pos} size=#{mfile.size}", cause: cause)
      end
    end
  end
end
