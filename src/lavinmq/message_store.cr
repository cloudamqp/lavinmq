require "./mfile"
require "./filesystem"
require "./segment_position"
require "./rate_limiter"
require "log"
require "file_utils"
require "./clustering/server"
require "./persister"
require "./bool_channel"
require "./rough_time"
require "./message_store/requeued_store"

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
    PURGE_YIELD_INTERVAL = 16_384
    Log                  = LavinMQ::Log.for "message_store"
    @segments = Hash(UInt32, MFile).new
    @deleted = Hash(UInt32, Array(UInt32)).new
    @segment_msg_count = Hash(UInt32, UInt32).new(0u32)
    @requeued : RequeuedStore = PublishOrderedRequeuedStore.new
    @closed = false
    # Set at load when the segment written to has data that can't be read
    @wfile_unreadable = false
    # When the reader moved into @rfile, see #read_fast?
    @rfile_since : Time::Instant = RoughTime.instant
    # Set once a publish to this store is synced, see #readahead_for_writes
    @synced_writes = false
    getter closed
    getter bytesize = 0u64
    getter size = 0u32
    getter empty = BoolChannel.new(true)

    def initialize(@msg_dir : String, replicator : Clustering::Replicator?, durable : Bool = true,
                   metadata : ::Log::Metadata = ::Log::Metadata.empty, persister : Persister? = nil)
      @log = Logger.new(Log, metadata)
      @durable = durable
      # Non-durable queues unlink their files at creation, so they cannot be
      # replicated by reading from disk. Skip replication entirely for them.
      @replicator = durable ? replicator : nil
      @persister = durable ? persister : nil
      @acks = Hash(UInt32, MFile).new { |acks, seg| acks[seg] = open_ack_file(seg) }
      # Another instance can have emptied segments in the same directory,
      # e.g. the store of a queue that is restarted
      @persister.try &.flush_unlinks(@msg_dir)
      close_on_error do
        load_segments_from_disk
        load_acks_from_disk
      end
      @wfile_id = @segments.last_key
      @wfile = @segments.last_value
      @rfile_id = @segments.first_key
      @rfile = @segments.first_value
      close_on_error do
        load_stats_from_segments
        prune_orphaned_acks
        delete_unused_segments
        seal_unreadable_write_segment
        readahead_for_writes(@wfile)
        @empty.set empty?
      end
    end

    # Nothing more is written to a segment with data that can't be read: it
    # couldn't be read either, and would be removed with the segment when a
    # purge gives the data up
    private def seal_unreadable_write_segment : Nil
      open_new_segment if @wfile_unreadable
      @wfile_unreadable = false
    end

    # Unmaps the segments and ack files opened before a failure while
    # loading, the caller gets no store to close. Doesn't call #close, which
    # subclasses override to close what they set up after super.
    private def close_on_error(&)
      yield
    rescue ex
      close_store
      raise ex
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
      was_empty = @size.zero?
      @requeued.insert(sp)
      @bytesize += sp.bytesize
      @size += 1
      @empty.set false if was_empty
    end

    def first? : Envelope? # ameba:disable Metrics/CyclomaticComplexity
      raise ClosedError.new if @closed
      while sp = @requeued.first?
        seg = @segments[sp.segment]? || next drop_requeued
        begin
          msg = BytesMessage.from_bytes(seg.to_slice + sp.position)
          return Envelope.new(sp, msg, redelivered: true, segment: seg)
        rescue ex
          raise unreadable(sp.segment, seg, ex)
        end
      end

      loop do
        seg = @rfile_id
        rfile = @rfile
        pos = rfile.pos.to_u32
        if pos == rfile.size # EOF?
          select_next_read_segment && next
          return if @size.zero?
          recount_from_segments
          return
        end
        verify_schema(rfile) if pos == 4
        if deleted?(seg, pos)
          BytesMessage.skip(rfile)
          next
        end
        msg = BytesMessage.from_bytes(rfile.to_slice + pos)
        raise IndexError.new("Message at segment #{seg} pos #{pos} has zero timestamp") if msg.timestamp.zero?
        sp = SegmentPosition.make(seg, pos, msg)
        return Envelope.new(sp, msg, redelivered: false, segment: rfile)
      rescue ex : IndexError
        @log.warn(exception: ex) { "Msg file size does not match expected value, moving on to next segment" }
        select_next_read_segment && next
        return if @size.zero?
        raise Error.new(@rfile, cause: ex)
      rescue ex
        @log.error(exception: ex) { "first? failed: #{state_snapshot}" }
        raise unreadable(@rfile_id, @rfile, ex)
      end
    end

    # Shifts the next message, under `lock`, and yields it outside the lock with
    # its segment kept mapped until the block returns, even if the segment is
    # deleted or the store closed meanwhile. For deliveries, which can be
    # suspended in a socket write. Returns false if there was no message.
    def shift_with_lease?(lock : Mutex, consumer = nil, & : Envelope -> _) : Bool
      env = lock.synchronize { shift?(consumer).try &.lease } || return false
      begin
        yield env
      ensure
        env.release
      end
      true
    end

    # The envelope points into the segment, so it's only valid while the lock
    # guarding the store is held, see #shift_with_lease? for using it outside it
    def shift?(consumer = nil) : Envelope? # ameba:disable Metrics/CyclomaticComplexity
      raise ClosedError.new if @closed
      while sp = @requeued.first?
        segment = @segments[sp.segment]? || next drop_requeued
        begin
          msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
        rescue ex
          # The message stays requeued, so that the read can be tried again
          raise unreadable(sp.segment, segment, ex)
        end
        drop_requeued
        return Envelope.new(sp, msg, redelivered: true, segment: segment)
      end

      loop do
        rfile = @rfile
        seg = @rfile_id
        pos = rfile.pos.to_u32
        if pos == rfile.size # EOF?
          select_next_read_segment && next
          return if @size.zero?
          recount_from_segments
          return
        end
        verify_schema(rfile) if pos == 4
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
        return Envelope.new(sp, msg, redelivered: false, segment: rfile)
      rescue ex : IndexError
        @log.warn(exception: ex) { "Msg file size does not match expected value, moving on to next segment" }
        select_next_read_segment && next
        return if @size.zero?
        raise Error.new(@rfile, cause: ex)
      rescue ex
        @log.error(exception: ex) { "shift? failed: #{state_snapshot}" }
        raise unreadable(@rfile_id, @rfile, ex)
      end
    end

    # The error for a read that failed in segment *seg*. If that's the segment
    # written to, the next write starts a new one, see
    # #seal_unreadable_write_segment.
    private def unreadable(seg : UInt32, mfile : MFile, cause : Exception) : Error
      error = Error.new(mfile, cause: cause)
      open_new_segment if seg == @wfile_id && !@closed
      error
    end

    # Removes the first requeued message, e.g. one whose segment is gone, which
    # can never be read
    private def drop_requeued : Nil
      sp = @requeued.shift? || return
      @bytesize -= sp.bytesize
      @size -= 1
      @empty.set true if @size.zero?
    end

    # A segment's records are only read if it has this version's format. It's
    # checked when a read starts the segment, as a load keeps a segment it
    # can't read (see #count_unreadable).
    private def verify_schema(mfile : MFile) : Nil
      raise OutdatedSchemaVersion.new(schema_version(mfile), mfile.path) unless schema_ok?(mfile)
    end

    private def schema_ok?(mfile : MFile) : Bool
      schema_version(mfile) == Schema::VERSION
    end

    private def schema_version(mfile : MFile) : Int32
      IO::ByteFormat::SystemEndian.decode(Int32, mfile.to_slice(0, sizeof(Int32)))
    end

    def [](sp : SegmentPosition) : BytesMessage
      raise ClosedError.new if @closed
      segment = @segments[sp.segment]? || raise MissingSegment.new(sp)
      begin
        BytesMessage.from_bytes(segment.to_slice + sp.position)
      rescue ex
        raise Error.new(segment, cause: ex)
      end
    end

    # Like `#[]`, as an envelope, with the segment position made from the message
    def envelope(sp : SegmentPosition, redelivered = false) : Envelope
      raise ClosedError.new if @closed
      segment = @segments[sp.segment]? || raise MissingSegment.new(sp)
      begin
        msg = BytesMessage.from_bytes(segment.to_slice + sp.position)
        Envelope.new(SegmentPosition.make(sp.segment, sp.position, msg), msg, redelivered: redelivered, segment: segment)
      rescue ex
        raise Error.new(segment, cause: ex)
      end
    end

    def delete(sp) : Nil
      raise ClosedError.new if @closed
      # A message routed outside the lock can have its segment purged meanwhile,
      # and @acks would otherwise create an ack file for the deleted segment
      return unless @segments.has_key?(sp.segment)
      afile = @acks[sp.segment]
      begin
        ack_offset = afile.size.to_i64
        afile.write_bytes sp.position
        @replicator.try &.append_value(afile.path, sp.position, ack_offset)

        # if all msgs in a segment are deleted then delete the segment
        return if sp.segment == @wfile_id # don't try to delete a segment we still write to
        ack_count = afile.size // sizeof(UInt32)
        msg_count = @segment_msg_count[sp.segment]
        if ack_count == msg_count
          @log.debug { "Deleting segment #{sp.segment} (delete sp): #{state_snapshot}" }
          select_next_read_segment if sp.segment == @rfile_id
          unlink_segment(sp.segment)
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
      while count < max_count
        env = shift_or_give_up? || break
        delete(env.segment_position)
        count += 1
        break if count >= max_count
        Fiber.yield if (count % PURGE_YIELD_INTERVAL).zero?
      end
      count
    end

    # A purge gives up data that can't be read, which is the way to get rid of
    # it: the read is tried again after what it failed on is removed
    private def shift_or_give_up? : Envelope?
      loop do
        return shift?
      rescue ex : Error
        give_up_unreadable(ex)
      end
    end

    # Removes the segment of what the next read can't decode: the first
    # requeued message, else the segment being read. All of the segment goes,
    # as where the record after one that can't be decoded starts is unknown,
    # and an ack of it wouldn't let a load skip it either. That includes the
    # segment's unacked messages, whose acks are ignored later (see #delete),
    # and its requeued ones.
    private def give_up_unreadable(ex : Error) : Nil
      seg = @requeued.first?.try(&.segment) || @rfile_id
      @log.warn(exception: ex) { "Purging segment #{seg}, which has data that can't be read" }
      drop_segment(seg)
      recount_ready
    end

    # Written to a segment's ack file when the segment is given up. Its files
    # are unlinked later (see #unlink_segment), and if that doesn't happen, a
    # crash for example, the next load removes the segment instead of
    # finding the data that can't be read again.
    GIVEN_UP = UInt32::MAX

    private def drop_segment(seg : UInt32) : Nil
      return unless @segments.has_key?(seg)
      mark_given_up(seg)
      open_new_segment if seg == @wfile_id
      select_next_read_segment if seg == @rfile_id
      unlink_segment(seg)
      @segment_msg_count.delete(seg)
      @deleted.delete(seg)
      @requeued.reject! { |sp| sp.segment == seg }
    end

    private def mark_given_up(seg : UInt32) : Nil
      return unless @durable
      afile = @acks[seg]
      ack_offset = afile.size.to_i64
      afile.write_bytes GIVEN_UP
      @replicator.try &.append_value(afile.path, GIVEN_UP, ack_offset)
    end

    private def given_up?(seg : UInt32) : Bool
      @deleted[seg]?.try(&.last?) == GIVEN_UP
    end

    # Counts the messages that are ready again, after a segment was removed
    # with an unknown number of them: the requeued ones and those not yet read
    private def recount_ready : Nil
      @size = 0u32
      @bytesize = 0u64
      @requeued.each do |sp|
        @size += 1
        @bytesize += sp.bytesize
      end
      count_unread
      @empty.set @size.zero?
    end

    # Counts what's left to read from where the reader is, like a load does
    private def count_unread : Nil
      @segments.each do |seg, mfile|
        next if seg < @rfile_id
        read_pos = mfile.pos
        begin
          mfile.pos = 4 unless seg == @rfile_id
          loop do
            pos = mfile.pos
            break if pos >= mfile.size
            ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
            break if ts.zero?
            bytesize = BytesMessage.skip(mfile)
            next if deleted?(seg, pos)
            @size += 1
            @bytesize += bytesize
          end
        rescue IO::EOFError
        rescue OverflowError | AMQ::Protocol::Error::FrameDecode
          # Data that can't be read, counted as at load, see #count_unreadable
          @size += 1
          @bytesize += mfile.size - mfile.pos
        ensure
          mfile.pos = read_pos
        end
      end
    end

    def purge_all
      # Drain @requeued, acking each entry so it doesn't come back after a restart
      while sp = @requeued.shift?
        @size -= 1
        @bytesize -= sp.bytesize
        delete(sp) if @segments.has_key?(sp.segment)
      end

      # Delete all segments except the current rfile and wfile
      @segments.reject! do |seg_id, file|
        next false if seg_id == @rfile_id || seg_id == @wfile_id

        # Only decrement @size for unread segments. Read segments don't
        # contribute to @size: their msgs are either acked, in-flight, or
        # were drained from @requeued above.
        if (msg_count = @segment_msg_count.delete(seg_id)) && seg_id > @rfile_id
          # msg_count is every msg ever written to the segment, acked ones
          # included, while @size only counts the ready ones. Never take off
          # more than @size holds: which msgs are ready is the subclass's to
          # decide, and a purge must not underflow the counter here, outside
          # the rescue below.
          acked = @acks[seg_id]?.try { |f| (f.size // sizeof(UInt32)).to_u32 } || 0u32
          ready = acked < msg_count ? msg_count - acked : 0u32
          @size -= Math.min(ready, @size)
        end
        unlink_files(file, @acks.delete(seg_id))
        @deleted.delete(seg_id)
        true
      end

      # Purge @rfile and @wfile. Swallow any inconsistency between @size and the
      # on-disk segments — purge_all's postcondition is an empty store, so we
      # force the counters to zero below regardless of how the loop exits.
      begin
        while env = shift_or_give_up?
          delete(env.segment_position)
        end
      rescue ex : IO::EOFError | Error
        @log.warn(exception: ex) { "purge_all: store state out of sync with segments, resetting counters" }
      end

      # The msgs are gone, so their counts in the meta files are too
      delete_meta_files

      @bytesize = 0_u64
      @size = 0_u32
      @empty.set true
    end

    private def delete_meta_files : Nil
      @segments.each_value do |file|
        meta = meta_file_name(file)
        next unless File.exists?(meta)
        @replicator.try &.delete_file(meta)
        File.delete?(meta)
      end
    end

    def delete
      @closed = true
      @empty.close
      # Not left to the persister like an emptied segment: a queue's delete
      # is durable in the definitions before its store is deleted, so its
      # files have nothing left to recover. The messages moved out of them
      # are only in their destinations now, so those are synced soon.
      @persister.try &.drain_soon
      @segments.reject! { |_, f| delete_file(f, including_meta: true); true }
      @acks.reject! { |_, f| delete_file(f); true }
      FileUtils.rm_rf @msg_dir
    end

    # Messages moved out of a segment (dead-lettered, delayed, retried) can be
    # unsynced in their destinations, and the unlink must not become durable
    # first, so the persister unlinks the files, after its next sync. A
    # non-durable store has none, and no copy to wait for.
    private def unlink_segment(seg_id : UInt32) : Nil
      if segment = @segments.delete(seg_id)
        unlink_files(segment, @acks.delete(seg_id))
      elsif acks = @acks.delete(seg_id)
        delete_file(acks)
      end
    end

    private def unlink_files(segment : MFile, acks : MFile?) : Nil
      if persister = @persister
        persister.unlink_after_sync(segment, meta_file_name(segment), acks)
      else
        # The segment first, see Persister#perform
        delete_file(segment, including_meta: true)
        acks.try { |file| delete_file(file) }
      end
    end

    # Segment files are the source of truth: count their msgs and rewrite the
    # meta files, as a load does without them
    private def recount_from_segments : Nil
      @log.warn { "#{@size} msgs counted but none left to read, recounting from the segment files" }
      @segments.each do |seg, mfile|
        read_pos = mfile.pos
        count_messages(seg, mfile)
        mfile.pos = read_pos
      end
      readahead_for_writes(@wfile) # count_messages left it advised for random access
      # count_messages adds the msgs that aren't acked to the counters, as at
      # load, but all of them have been read
      @size = 0u32
      @bytesize = 0u64
      @empty.set true
      # Segments whose msgs turned out to all be acked
      delete_unused_segments
    end

    private def delete_file(file : MFile, including_meta = false)
      file.delete(raise_on_missing: false)
      if replicator = @replicator
        replicator.delete_file(meta_file_name(file)) if including_meta
        replicator.delete_file(file.path)
      end
      File.delete?(meta_file_name(file)) if including_meta
      # A delivery may still be reading from the mapping, see Envelope#lease
      file.close
    end

    def empty?
      @size.zero?
    end

    def close : Nil
      close_store
    end

    private def close_store : Nil
      return if @closed
      @closed = true
      @empty.close
      # A delivery may still be reading from a segment, e.g. a basic.get that
      # isn't waited for like consumers are, MFile#close only unmaps it once
      # the delivery releases it (see Envelope#lease)
      if replicator = @replicator
        @segments.each_value do |segment|
          replicator.register_file segment.path
          segment.close
        end
        @acks.each_value do |ackfile|
          replicator.register_file ackfile.path
          ackfile.close
        end
      else
        @segments.each_value &.close
        @acks.each_value &.close
      end
    end

    # Called without the queue's lock (HTTP API, stats), so @size and
    # @bytesize can change in between the reads
    def avg_bytesize : UInt32
      size = @size
      return 0u32 if size.zero?
      Math.min(@bytesize // size, UInt32::MAX).to_u32
    end

    private def state_snapshot : String
      String.build do |io|
        io << "store_closed=" << @closed
        io << " rfile_id=" << @rfile_id
        io << " rfile=" << @rfile.path << "(closed=" << @rfile.closed? << " pos=" << @rfile.pos << " size=" << @rfile.size << ")"
        io << " wfile_id=" << @wfile_id
        io << " segments=" << @segments.keys
        io << " size=" << @size
      end
    end

    private def select_next_read_segment : MFile?
      # Nothing follows the segment being written. A consumer that has caught
      # up with the publisher gets here on every take, so this must not scan
      # the segments or drop the pages the publisher is about to write
      return if @rfile_id == @wfile_id
      prev_id = @rfile_id
      # Expect @segments to be ordered
      if id = @segments.each_key.find { |sid| sid > @rfile_id }
        @rfile.dontneed unless @rfile.closed?
        rfile = @segments[id]
        # Not the segment being written, it's read as it's written and cached
        if id != @wfile_id && read_fast?(@rfile, @rfile_since)
          rfile.advise(MFile::Advice::Sequential)
        end
        @rfile_id = id
        @rfile = rfile
        @rfile_since = RoughTime.instant
        @log.debug { "select_next_read_segment: #{prev_id} -> #{id}, segments=#{@segments.keys}" }
        rfile
      else
        @log.debug { "select_next_read_segment: no segment > #{prev_id}, segments=#{@segments.keys}, rfile_closed=#{@rfile.closed?}" }
        nil
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
      @replicator.try &.append(wfile.path, sp.position, wfile.size - sp.position)
      # After the replication dispatch, so the fsync request the persister
      # sends followers comes after this append in the stream
      if (msg.needs_sync? || msg.moved?) && (persister = @persister)
        persister.mark_dirty(wfile)
      end
      random_access_for_sync(wfile) if !@synced_writes && synced?(msg)
      @segment_msg_count[wfile_id] += 1
      sp
    end

    private def synced?(msg) : Bool
      (msg.needs_sync? || msg.synced_by_commit?) && @durable && Config.instance.sync?
    end

    # The segment being written is mapped with readahead, which caches it in
    # large folios and halves the CPU time of writing it. But an msync writes
    # whole folios, so a confirm would write up to 128 KiB for a small
    # message. Once a publish to the store is synced, its write segments are
    # mapped without readahead and a sync only writes the pages that changed.
    private def readahead_for_writes(wfile : MFile) : Nil
      wfile.advise(MFile::Advice::Normal) unless @synced_writes
    end

    private def random_access_for_sync(wfile : MFile) : Nil
      @synced_writes = true
      wfile.advise(MFile::Advice::Random)
    end

    # Segments are mapped without readahead. Readahead on a page fault caches
    # up to the readahead window (4 MiB on btrfs, 128 KiB on ext4/XFS) around
    # it: for a slow reader the part it hasn't reached yet, kept for as long
    # as the queue is read, which with many queues adds up to gigabytes.
    # Fast readers get readahead back, see #read_fast?, and so does loading
    # at startup, see #load_stats_from_segments. The segment being written
    # is the exception, see #readahead_for_writes.
    private def open_segment(path : String, capacity : Int? = nil) : MFile
      mfile = MFile.new(path, capacity)
      mfile.advise(MFile::Advice::Random)
      mfile
    end

    # The last segment isn't full, so it's mapped with room to write to, and
    # it has no metadata file, so it's read through to count its messages
    # (see #load_stats_from_segments): fetch it in one readahead instead of
    # a page fault at a time. A segment a crash left at its capacity is
    # sparse past its data, so the readahead is bounded by what's allocated.
    # Only on Linux: APFS allocates a segment's whole capacity on its first
    # msync, so there the bound doesn't hold, and the readahead made stops
    # and starts after a crash slower on macOS.
    private def open_last_segment(path : String) : MFile
      mfile = open_segment(path, Config.instance.segment_size)
      {% if flag?(:linux) %}
        length = Math.min(mfile.size, mfile.allocated_size)
        mfile.advise(MFile::Advice::WillNeed, length: length) unless length.zero?
      {% end %}
      mfile
    end

    # A reader that got through its previous segment at least this fast gets
    # readahead for its next one. A slower reader isn't waiting on the disk,
    # so readahead wouldn't speed it up. Reading a segment a page per fault,
    # without readahead, is faster than this even on slow disks, so a reader
    # that is held back by the disk still gets it.
    READAHEAD_MIN_BYTES_PER_SECOND = 1024 * 1024

    private def read_fast?(segment : MFile, since : Time::Instant) : Bool
      segment.size >= READAHEAD_MIN_BYTES_PER_SECOND * (RoughTime.instant - since).total_seconds
    end

    # Called on rollover for the segment that was just written to
    private def unmap_finished_segment(seg : UInt32, mfile : MFile) : Nil
      mfile.dontneed unless mfile == @rfile
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      unless @wfile_id.zero?
        write_metadata_file(@wfile_id, @wfile)
        @wfile.truncate(@wfile.size)
        @wfile.advise(MFile::Advice::Random) unless @synced_writes
      end
      unmap_finished_segment(@wfile_id, @wfile)
      next_id = @wfile_id + 1
      path = File.join(@msg_dir, "msgs.#{next_id.to_s.rjust(10, '0')}")
      capacity = Math.max(Config.instance.segment_size, next_msg_size + 4)
      wfile = open_segment(path, capacity)
      readahead_for_writes(wfile)
      wfile.write_bytes Schema::VERSION
      wfile.pos = 4
      @replicator.try &.register_file wfile
      @replicator.try &.append_value path, Schema::VERSION, 0i64
      @wfile_id = next_id
      @wfile = @segments[next_id] = wfile
      delete_unused_segments
      @wfile.delete unless @durable # mark as deleted if non-durable
      wfile
    end

    private def write_metadata_file(seg : UInt32, wfile : MFile)
      metafile = meta_file_name(wfile)
      @log.debug { "Write message segment meta file #{metafile}" }
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
      # Page sized folios, so a sync doesn't rewrite up to 128 KiB of acks
      # for each 4 byte append (see #open_segment). A page fault
      # still covers 1024 acks, so it's cheap enough to always do.
      mfile.advise(MFile::Advice::Random)
      mfile.delete unless @durable # mark as deleted if non-durable
      @replicator.try &.register_file mfile
      mfile
    end

    private def load_acks_from_disk : Nil
      count = 0u32
      Dir.each_child(@msg_dir) do |f|
        if f.starts_with?("tmp.acks.")
          stale = File.join(@msg_dir, f)
          @log.debug { "Deleting stale ack rewrite tempfile: #{stale}" }
          File.delete?(stale)
          next
        end
        next unless f.starts_with?("acks.")
        seg = f[5, 10].to_u32
        path = File.join(@msg_dir, f)

        # The ack file is orphaned if there is no corresponding msg file
        msgs_path = File.join(@msg_dir, "msgs.#{seg.to_s.rjust(10, '0')}")
        unless File.exists?(msgs_path)
          @log.warn { "Deleting orphaned ack file: #{path}" }
          File.delete(path)
          @replicator.try &.delete_file(path)
          next
        end

        acked = Array(UInt32).new
        File.open(path, "a+") do |file|
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
        @deleted[seg] = acked.sort! unless acked.empty?
        count += 1
        Fiber.yield
      end
      @log.debug { "Loaded #{count} ack files" }
    rescue File::NotFoundError
      # msg_dir does not exist, nothing to load
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
        file = idx == last_idx ? open_last_segment(path) : open_segment(path)
        @replicator.try &.register_file file
        file.delete unless @durable # mark files for non-durable queues for deletion

        if was_empty
          file.write_bytes Schema::VERSION
          @replicator.try &.append_value path, Schema::VERSION, 0i64
        else
          begin
            SchemaVersion.verify(file, :message)
          rescue IO::EOFError
            # delete empty file, it will be recreated if it's needed
            @log.warn { "Empty file at #{path}, deleting it" }
            delete_file(file, including_meta: true)
            if idx == 0 # Recreate the file if it's the first segment because we need at least one segment to exist
              file = open_segment(path, Config.instance.segment_size)
              file.write_bytes Schema::VERSION
              @replicator.try &.append_value path, Schema::VERSION, 0i64
            else
              @segments.delete seg
              next
            end
          rescue ex
            # Kept, and counted as unreadable (see #load_stats_from_segments)
            @log.error { "Segment #{seg} can't be read: #{ex.message}" }
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
        @log.info { "Loading #{@segments.size} segments" }
      else
        @log.debug { "Loading #{@segments.size} segments" }
      end
      log_limiter = RateLimiter.new(2.seconds)
      @segments.each do |seg, mfile|
        if given_up?(seg)
          # A purge gave the segment up, but it wasn't unlinked. Without msgs
          # it's deleted as unused, the one written to once it's sealed.
          @log.info { "Removing segment #{seg}, which a purge gave up" }
          @segment_msg_count[seg] = 0u32
          @wfile_unreadable = true if seg == @wfile_id
        elsif !schema_ok?(mfile)
          count_unreadable(seg, mfile, 4)
        elsif File.exists?(meta_file_name(mfile))
          begin
            if @deleted.has_key?(seg)
              # Skipping the acked messages reads the segment in order.
              # Without readahead (see #open_segment) that would fault it in
              # a page at a time.
              with_advice(mfile, MFile::Advice::Sequential) { read_metadata_file(seg, mfile) }
            else
              read_metadata_file(seg, mfile)
            end
          rescue MetadataError
            count_messages(seg, mfile)
          end
        else
          count_messages(seg, mfile)
        end
        counter &+= 1
        if is_long_queue
          log_limiter.do { @log.info { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } }
        else
          @log.debug { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } if counter % 128 == 0
        end
      end
      @log.info { "Loaded #{counter} segments, #{@size} messages" }
    end

    # Reads a segment through to count its messages, with readahead. The
    # last segment was fetched when it was opened (see #open_last_segment),
    # but only as far as it's allocated, which on a compressing filesystem is
    # less than its data, so it's read ahead here too. It isn't full yet, so
    # it gets no metadata file.
    private def count_messages(seg, mfile)
      readable = with_advice(mfile, MFile::Advice::Sequential) { produce_metadata(seg, mfile) }
      # A segment with unreadable data gets no metadata file, it's counted
      # again by the next load
      write_metadata_file(seg, mfile) if readable && seg != @segments.last_key
    end

    # The rest of segment *seg* from *pos* can't be decoded, at load. The
    # segment is kept and the data is left for a read to fail on, which pauses
    # the queue. It's counted as one message, so that the store isn't empty
    # and it is read. The segment gets a message count that acks can't reach,
    # so it isn't deleted before a purge gives the data up.
    private def count_unreadable(seg : UInt32, mfile : MFile, pos : Int, readable_unacked : UInt32 = 0u32) : Nil
      acked = @deleted[seg]?.try(&.size.to_u32) || 0u32
      @segment_msg_count[seg] = acked + readable_unacked + 1
      @wfile_unreadable = true if seg == @wfile_id
      @bytesize += mfile.size - pos
      @size += 1
    end

    # Advises *mfile* for the block, then back to random access (see
    # #open_segment), also when the block raises
    private def with_advice(mfile : MFile, advice : MFile::Advice, &)
      mfile.advise(advice)
      yield
    ensure
      mfile.advise(MFile::Advice::Random) unless mfile.closed?
    end

    # Overridden by specs to see how a segment is advised and cached as its
    # load starts (AdviceOnLoadStore in spec/message_store_spec.cr)
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
      # Every msg is at least MIN_BYTESIZE, so the msgs left in the segment have
      # to fit in the bytes the acked msgs don't take up. If they don't, the
      # meta file counts msgs that the segment doesn't hold.
      if count.to_u64 * BytesMessage::MIN_BYTESIZE > bytesize
        raise MetadataError.new("Metadata file #{metafile} counts #{count} msgs left in #{bytesize} bytes")
      end
      mfile.pos = 4
      mfile.dontneed
      @bytesize += bytesize
      @size += count
      @log.debug { "Reading count from #{metafile}: #{count}" }
    rescue ex : File::NotFoundError
      raise ex
    rescue ex
      @log.error(exception: ex) { "Metadata file #{metafile} is incorrect" }
      raise MetadataError.new("Metadata file #{metafile} is incorrect", cause: ex)
    end

    private def read_extra_metadata_fields(file : File, seg : UInt32)
      # Used in subclasses of MessageStore to read additional metadata fields
    end

    # Removes ack positions referencing data past the reconstructed segment size.
    # Such orphaned acks can survive an unclean shutdown when kernel page-cache
    # flushes the acks file but not the corresponding msgs file, and would
    # otherwise cause shift? to skip over brand-new messages as if acked.
    private def prune_orphaned_acks : Nil
      # Snapshot keys — prune_orphaned_acks_for_segment mutates @deleted.
      @deleted.keys.each do |seg|
        next unless positions = @deleted[seg]?
        next unless mfile = @segments[seg]?
        prune_orphaned_acks_for_segment(seg, mfile, positions)
      end
    end

    private def prune_orphaned_acks_for_segment(seg : UInt32, mfile : MFile, positions : Array(UInt32)) : Nil
      data_end = mfile.size.to_u32
      valid = positions.select { |p| p < data_end || p == GIVEN_UP }
      orphan_count = positions.size - valid.size
      return if orphan_count.zero?

      @log.warn do
        "Msgs/acks files for segment #{seg} are out of sync (possibly because of " \
        "an unclean shutdown). Removing #{orphan_count} orphaned ack position(s)."
      end

      if valid.empty?
        @deleted.delete(seg)
      else
        @deleted[seg] = valid
      end

      rewrite_ack_file(seg, valid)
    end

    private def rewrite_ack_file(seg : UInt32, positions : Array(UInt32)) : Nil
      return unless @durable

      basename = "acks.#{seg.to_s.rjust(10, '0')}"
      final_path = File.join(@msg_dir, basename)
      tmp_path = File.join(@msg_dir, "tmp.#{basename}")

      File.open(tmp_path, "w") do |f|
        positions.each { |p| f.write_bytes(p, IO::ByteFormat::SystemEndian) }
      end

      # Unmap the old file before renaming so mmap stops pinning the old inode.
      if old = @acks.delete(seg)
        old.close(truncate_to_size: false)
      end

      FileSystem.durable_rename(tmp_path, final_path)

      # Ship the rewritten (short) file to followers before reopening, so
      # ReplaceAction captures the post-rename file size rather than the
      # capacity-sized file produced by open_ack_file's ftruncate.
      @replicator.try &.replace_file(final_path)
      @acks[seg] = open_ack_file(seg)
    end

    # Returns false if the segment has data that can't be read.
    # Overridden by specs, like #read_metadata_file
    private def produce_metadata(seg, mfile) : Bool
      count = 0u32
      unacked = 0u32
      mfile.pos = 4
      loop do
        pos = mfile.pos
        ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
        break mfile.resize(pos) if ts.zero? # This means that the rest of the file is zero, so resize it
        bytesize = BytesMessage.skip(mfile)
        count += 1
        next if deleted?(seg, pos)
        unacked += 1
        @bytesize += bytesize
        @size += 1
      rescue IO::EOFError
        # EOF at a record boundary is the normal end of an intact segment;
        # anything before it is an incomplete record to drop.
        if pos && pos < mfile.size
          mfile.resize(pos)
          @replicator.try &.replace_file(mfile) # followers append at their own end
          @log.warn { "Dropping incomplete trailing record in segment #{seg} at pos #{pos}" }
        end
        break
      rescue ex : OverflowError | AMQ::Protocol::Error::FrameDecode
        pos ||= mfile.pos
        @log.error { "Segment #{seg} can't be read from pos #{pos}: #{ex}" }
        count_unreadable(seg, mfile, pos, unacked)
        mfile.pos = 4
        return false
      end
      mfile.pos = 4
      mfile.dontneed
      Fiber.yield
      @segment_msg_count[seg] = count
      @log.debug { "Manually counted #{count} msgs from #{mfile.path}" }
      true
    end

    private def delete_unused_segments : Nil
      current_seg = @segments.last_key
      @segments.reject! do |seg, mfile|
        next if seg == current_seg # don't the delete the segment still being written to

        if (acks = @acks[seg]?) && @segment_msg_count[seg] <= (acks.size // sizeof(UInt32))
          @log.debug { "Deleting unused segment #{seg}: #{state_snapshot}" }
          select_next_read_segment if seg == @rfile_id
          @segment_msg_count.delete seg
          @deleted.delete seg
          unlink_files(mfile, @acks.delete(seg))
          true
        else
          false
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

    # The segment of a message is gone, and the message with it: a purge
    # removed it, see #give_up_unreadable
    class MissingSegment < KeyError
      def initialize(sp : SegmentPosition)
        super("Segment #{sp.segment} of #{sp} is gone")
      end
    end

    class Error < Exception
      def initialize(mfile : MFile, cause = nil)
        super("path=#{mfile.path} pos=#{mfile.pos} size=#{mfile.size}", cause: cause)
      end
    end
  end
end
