require "../mfile"
require "../segment_position"
require "log"
require "file_utils"

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
      Log = ::Log.for("MessageStore")
      @segments = Hash(UInt32, MFile).new
      @acks = Hash(UInt32, MFile).new
      @deleted = Hash(UInt32, Array(UInt32)).new
      @segment_msg_count = Hash(UInt32, UInt32).new
      @requeued = Deque(SegmentPosition).new
      @closed = false
      getter bytesize = 0u64
      getter size = 0u32
      getter empty_change = Channel(Bool).new

      def initialize(@data_dir : String)
        Dir.mkdir_p @data_dir
        load_segments_from_disk
        load_deleted_from_disk
        load_stats_from_segments
        @wfile_id = @segments.last_key
        @wfile = @segments.last_value
        @rfile_id = @segments.first_key
        @rfile = @segments.first_value
      end

      def push(msg, store_offset = false) : SegmentPosition
        raise ClosedError.new if @closed
        sp = write_to_disk(msg, store_offset)
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
          msg = BytesMessage.from_bytes(seg.to_slice(sp.position, sp.bytesize))
          return Envelope.new(sp, msg, redelivered: true)
        end

        loop do
          seg = @rfile_id
          rfile = @rfile
          pos = rfile.pos.to_u32
          if pos == rfile.size # EOF?
            select_next_read_segment ? next : return
          end
          if deleted?(seg, pos)
            BytesMessage.skip(rfile)
            next
          end
          msg = BytesMessage.from_bytes(rfile.to_slice(pos, rfile.size - pos))
          sp = SegmentPosition.make(seg, pos, msg)
          return Envelope.new(sp, msg, redelivered: false)
        end
      rescue ex
        Log.error(exception: ex) { "rfile=#{@rfile.path} pos=#{@rfile.pos} size=#{@rfile.size}" }
        raise ex
      end

      def shift? : Envelope?
        raise ClosedError.new if @closed
        if sp = @requeued.shift?
          segment = @segments[sp.segment]
          msg = BytesMessage.from_bytes(segment.to_slice(sp.position, sp.bytesize))
          @bytesize -= sp.bytesize
          @size -= 1
          notify_empty(true) if @size.zero?
          return Envelope.new(sp, msg, redelivered: true)
        end

        loop do
          rfile = @rfile
          seg = @rfile_id
          pos = rfile.pos.to_u32
          if pos == rfile.size # EOF?
            select_next_read_segment ? next : return
          end
          if deleted?(seg, pos)
            BytesMessage.skip(rfile)
            next
          end
          msg = BytesMessage.from_bytes(rfile.to_slice(pos, rfile.size - pos))
          sp = SegmentPosition.make(seg, pos, msg)
          rfile.seek(sp.bytesize, IO::Seek::Current)
          @bytesize -= sp.bytesize
          @size -= 1
          notify_empty(true) if @size.zero?
          return Envelope.new(sp, msg, redelivered: false)
        end
      rescue ex
        Log.error(exception: ex) { "rfile=#{@rfile.path} pos=#{@rfile.pos} size=#{@rfile.size}" }
        raise ex
      end

      def [](sp : SegmentPosition) : BytesMessage
        raise ClosedError.new if @closed
        segment = @segments[sp.segment]
        BytesMessage.from_bytes(segment.to_slice(sp.position, sp.bytesize))
      end

      def delete(sp) : Nil
        raise ClosedError.new if @closed
        afile = @acks[sp.segment]
        begin
          afile.write_bytes sp.position

          # if all msgs in a segment are deleted then delete the segment
          return if sp.segment == @wfile_id # don't try to delete a segment we still write to
          ack_count = afile.pos // sizeof(UInt32)
          msg_count = @segment_msg_count[sp.segment]
          if ack_count == msg_count
            select_next_read_segment if sp.segment == @rfile_id
            @acks.delete(sp.segment).try &.delete.close
            @segments.delete(sp.segment).try &.delete.close
            @segment_msg_count.delete(sp.segment)
          end
        rescue ex
          Log.error(exception: ex) { "#{ex} seg=#{sp.segment} pos=#{afile.pos} size=#{afile.size}" }
          raise ex
        end
      end

      # Resets the message store
      def purge : UInt32
        raise ClosedError.new if @closed
        @segments.each_value &.close
        @acks.each_value &.close
        @segments.clear
        @acks.clear
        @deleted.clear
        @segment_msg_count.clear
        @requeued.clear
        @bytesize = 0u64
        size = @size
        @size = 0u32
        open_new_segment
        @wfile_id = @segments.last_key
        @wfile = @segments.last_value
        @rfile_id = @segments.first_key
        @rfile = @segments.first_value
        notify_empty(true) unless size.zero?
        size
      end

      def delete
        close
        FileUtils.rm_rf @data_dir
      end

      def empty?
        @size.zero?
      end

      def close
        @closed = true
        @empty_change.close
        @segments.each_value &.close
        @acks.each_value &.close
      end

      def avg_bytesize : UInt32
        return 0u32 if @size.zero?
        (@bytesize / @size).to_u32
      end

      private def select_next_read_segment : MFile?
        # Expect @segments to be ordered
        if id = @segments.each_key.find { |sid| sid > @rfile_id }
          @rfile_id = id
          @rfile = @segments[id]
        end
      end

      private def write_to_disk(msg, store_offset = false) : SegmentPosition
        wfile = @wfile
        # Set x-offset before we have a SP so that the Message#bytesize is correct
        if store_offset
          headers = msg.properties.headers || AMQP::Table.new
          headers["x-offset"] = 0_i64
          msg.properties.headers = headers
        end
        if wfile.capacity < wfile.size + msg.bytesize
          wfile = open_new_segment(msg.bytesize)
        end
        wfile.seek(0, IO::Seek::End) do |pos|
          sp = SegmentPosition.make(@wfile_id, pos.to_u32, msg)
          if store_offset
            msg.properties.headers.not_nil!["x-offset"] = sp.to_i64
          end
          wfile.write_bytes msg
          @segment_msg_count[@wfile_id] += 1
          return sp
        end
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        # FIXME: fsync
        next_id = @segments.empty? ? 1_u32 : @wfile_id + 1
        filename = "msgs.#{next_id.to_s.rjust(10, '0')}"
        path = File.join(@data_dir, filename)
        capacity = Config.instance.segment_size + next_msg_size
        wfile = MFile.new(path, capacity)
        SchemaVersion.prefix(wfile, :message)
        @wfile_id = next_id
        @segment_msg_count[next_id] = 0u32
        @acks[next_id] = open_ack_file(next_id, capacity)
        @wfile = @segments[next_id] = wfile
      end

      private def open_ack_file(id, segment_capacity) : MFile
        filename = "acks.#{id.to_s.rjust(10, '0')}"
        max_msgs_per_segment = segment_capacity // BytesMessage::MIN_BYTESIZE
        capacity = (max_msgs_per_segment + 1) * sizeof(UInt32)
        mfile = MFile.new(File.join(@data_dir, filename), capacity)
        @acks[id] = mfile
      end

      private def load_deleted_from_disk
        @acks.each do |seg, afile|
          acked = Array(UInt32).new
          loop do
            pos = UInt32.from_io(afile, IO::ByteFormat::SystemEndian)
            break if pos.zero?
            acked << pos
          rescue IO::EOFError
            break
          end
          @deleted[seg] = acked.sort! unless acked.empty?
        end
      end

      private def load_segments_from_disk : Nil
        ids = Array(UInt32).new
        Dir.each_child(@data_dir) do |f|
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
          path = File.join(@data_dir, filename)
          file = if idx == last_idx
                   # expand the last segment
                   MFile.new(path, Config.instance.segment_size)
                 else
                   MFile.new(path)
                 end
          if was_empty
            SchemaVersion.prefix(file, :message)
          else
            SchemaVersion.verify(file, :message)
          end
          @segments[seg] = file
          @acks[seg] = open_ack_file(seg, file.capacity)
        end
      end

      # Populate bytesize, size and segment_msg_count
      private def load_stats_from_segments : Nil
        @segments.each do |seg, mfile|
          count = 0u32
          mfile.seek(4) do
            loop do
              pos = mfile.pos
              ts = IO::ByteFormat::SystemEndian.decode(Int64, mfile.to_slice(pos, 8))
              if ts.zero? # This means that the rest of the file is zero, so truncate it
                raise IO::EOFError.new
              end

              BytesMessage.skip mfile
              next if deleted?(seg, pos)

              @bytesize += mfile.pos - pos
              count += 1
            rescue IO::EOFError
              mfile.truncate(mfile.pos)
              break
            end
          end
          @segment_msg_count[seg] = count
          @size += count
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
    end
  end
end
