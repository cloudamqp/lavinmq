require "../../mfile"
require "../../segment_position"
require "log"
require "file_utils"
require "../../clustering/server"

module LavinMQ
  class Queue
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
      @requeued = Deque(SegmentPosition).new
      @closed = false
      getter bytesize = 0u64
      getter size = 0u32
      getter empty_change = Channel(Bool).new

      def initialize(@queue_data_dir : String, @replicator : Clustering::Replicator?, metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @log = Logger.new(Log, metadata)
        @acks = Hash(UInt32, MFile).new { |acks, seg| acks[seg] = open_ack_file(seg) }
        load_segments_from_disk
        load_deleted_from_disk
        load_stats_from_segments
        delete_unused_segments
        @wfile_id = @segments.last_key
        @wfile = @segments.last_value
        @rfile_id = @segments.first_key
        @rfile = @segments.first_value
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        sp = write_to_disk(msg)
        was_empty = @size.zero?
        @bytesize += sp.bytesize
        @size += 1
        notify_empty(false) if was_empty
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
        notify_empty(false) if was_empty
      end

      def first? : Envelope?
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
            notify_empty(true) if @size.zero?
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
          notify_empty(true) if @size.zero?
          return Envelope.new(sp, msg, redelivered: false)
        rescue ex : IndexError
          @log.warn { "Msg file size does not match expected value, moving on to next segment" }
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
              a.delete(raise_on_missing: false).close
              @replicator.try &.delete_file(a.path)
            end
            if seg = @segments.delete(sp.segment)
              seg.delete(raise_on_missing: false).close
              @replicator.try &.delete_file(seg.path)
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

      def delete
        close
        @segments.each_value { |f| delete_file(f) }
        @acks.each_value { |f| delete_file(f) }
        FileUtils.rm_rf @queue_data_dir
      end

      def delete_file(file)
        @replicator.try &.delete_file(file.path)
        file.delete(raise_on_missing: false)
        Fiber.yield
      end

      def empty?
        @size.zero?
      end

      def close : Nil
        @closed = true
        @empty_change.close
        @segments.each_value &.close
        @acks.each_value &.close
      end

      def avg_bytesize : UInt32
        return 0u32 if @size.zero?
        (@bytesize / @size).to_u32
      end

      def unmap_segments(except : Enumerable(UInt32) = StaticArray(UInt32, 0).new(0u32))
        @segments.each do |seg_id, mfile|
          next if mfile == @wfile
          next if except.includes? seg_id
          mfile.unmap
        end
      end

      private def select_next_read_segment : MFile?
        # Expect @segments to be ordered
        if id = @segments.each_key.find { |sid| sid > @rfile_id }
          @rfile_id = id
          @rfile = @segments[id]
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
        @replicator.try &.append(wfile.path, wfile, sp.position.to_i32, (wfile.size - sp.position).to_i32)
        @segment_msg_count[wfile_id] += 1
        sp
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        @wfile.unmap unless @wfile == @rfile
        next_id = @wfile_id + 1
        path = File.join(@queue_data_dir, "msgs.#{next_id.to_s.rjust(10, '0')}")
        capacity = Math.max(Config.instance.segment_size, next_msg_size + 4)
        delete_unused_segments
        wfile = MFile.new(path, capacity)
        wfile.write_bytes Schema::VERSION
        wfile.pos = 4
        @replicator.try &.register_file wfile
        @replicator.try &.append path, Schema::VERSION
        @wfile_id = next_id
        @wfile = @segments[next_id] = wfile
        wfile
      end

      private def open_ack_file(id) : MFile
        path = File.join(@queue_data_dir, "acks.#{id.to_s.rjust(10, '0')}")
        capacity = Config.instance.segment_size // BytesMessage::MIN_BYTESIZE * 4 + 4
        mfile = MFile.new(path, capacity, writeonly: true)
        @replicator.try &.register_file mfile
        mfile
      end

      private def load_deleted_from_disk
        count = 0u32
        ack_files = 0u32
        Dir.each(@queue_data_dir) do |f|
          ack_files += 1 if f.starts_with? "acks."
        end

        @log.debug { "Loading #{ack_files} ack files" }
        Dir.each_child(@queue_data_dir) do |child|
          next unless child.starts_with? "acks."
          seg = child[5, 10].to_u32
          acked = Array(UInt32).new
          File.open(File.join(@queue_data_dir, child), "a+") do |file|
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
          @log.debug { "Loaded #{count}/#{ack_files} ack files" } if (count += 1) % 128 == 0
          @deleted[seg] = acked.sort! unless acked.empty?
          Fiber.yield
        end
        @log.debug { "Loaded #{count} ack files" }
      end

      private def load_segments_from_disk : Nil
        ids = Array(UInt32).new
        Dir.each_child(@queue_data_dir) do |f|
          if f.starts_with? "msgs."
            ids << f[5, 10].to_u32
          end
        end
        ids.sort!
        was_empty = ids.empty?
        ids << 1_u32 if was_empty
        last_idx = ids.size - 1
        ids.each_with_index do |seg, idx|
          filename = "msgs.#{seg.to_s.rjust(10, '0')}"
          path = File.join(@queue_data_dir, filename)
          file = if idx == last_idx
                   # expand the last segment
                   MFile.new(path, Config.instance.segment_size)
                 else
                   MFile.new(path)
                 end
          @replicator.try &.register_file file
          if was_empty
            file.write_bytes Schema::VERSION
            @replicator.try &.append path, Schema::VERSION
          else
            begin
              SchemaVersion.verify(file, :message)
            rescue IO::EOFError
              # delete empty file, it will be recreated if it's needed
              @log.warn { "Empty file at #{path}, deleting it" }
              file.delete(raise_on_missing: false).close
              @replicator.try &.delete_file(path)
              if idx == 0 # Recreate the file if it's the first segment because we need at least one segment to exist
                file = MFile.new(path, Config.instance.segment_size)
                file.write_bytes Schema::VERSION
                @replicator.try &.append path, Schema::VERSION
              else
                @segments.delete seg
                next
              end
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
        @segments.each do |seg, mfile|
          count = 0u32
          loop do
            pos = mfile.pos
            ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
            break mfile.resize(pos) if ts.zero? # This means that the rest of the file is zero, so resize it
            bytesize = BytesMessage.skip(mfile)
            count += 1
            next if deleted?(seg, pos)
            update_stats_per_msg(seg, ts, bytesize)
          rescue ex : IO::EOFError
            break
          rescue ex : OverflowError | AMQ::Protocol::Error::FrameDecode
            raise Error.new(mfile, cause: ex)
          end
          mfile.pos = 4
          mfile.unmap # will be mmap on demand
          if is_long_queue
            @log.info { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
          else
            @log.debug { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
          end
          Fiber.yield
          @segment_msg_count[seg] = count
        end
        @log.info { "Loaded #{counter} segments, #{@size} messages" }
      end

      private def update_stats_per_msg(seg, ts, bytesize)
        @bytesize += bytesize
        @size += 1
      end

      private def delete_unused_segments : Nil
        current_seg = @segments.last_key
        @segments.reject! do |seg, mfile|
          next if seg == current_seg # don't the delete the segment still being written to

          if (acks = @acks[seg]?) && @segment_msg_count[seg] == (acks.size // sizeof(UInt32))
            @log.debug { "Deleting unused segment #{seg}" }
            @segment_msg_count.delete seg
            @deleted.delete seg
            if ack = @acks.delete(seg)
              ack.delete(raise_on_missing: false).close
              @replicator.try &.delete_file(ack.path)
            end
            mfile.delete(raise_on_missing: false).close
            @replicator.try &.delete_file(mfile.path)
            true
          end
          Fiber.yield
        end
      end

      private def deleted?(seg, pos) : Bool
        if del = @deleted[seg]?
          del.bsearch { |dpos| dpos >= pos } == pos
        else
          false
        end
      end

      private def notify_empty(is_empty)
        while @empty_change.try_send? is_empty
        end
      end

      class ClosedError < ::Channel::ClosedError; end

      class Error < Exception
        def initialize(mfile : MFile, cause = nil)
          super("path=#{mfile.path} pos=#{mfile.pos} size=#{mfile.size}", cause: cause)
        end
      end
    end
  end
end
