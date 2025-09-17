module LavinMQ
  module MessageStoreSegments
    private def select_next_read_segment : MFile?
      @rfile.dontneed
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

    private def delete_unused_segments : Nil
      current_seg = @segments.last_key
      @segments.reject! do |seg, mfile|
        next if seg == current_seg # don't the delete the segment still being written to

        if (acks = @acks[seg]?) && @segment_msg_count[seg] == (acks.size // sizeof(UInt32))
          @log.debug { "Deleting unused segment #{seg}" }
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

    private def deleted?(seg, pos) : Bool
      if del = @deleted[seg]?
        del.bsearch { |dpos| dpos >= pos } == pos
      else
        false
      end
    end

    private def load_deleted_from_disk
      count = 0u32
      ack_files = 0u32
      Dir.each(@msg_dir) do |f|
        ack_files += 1 if f.starts_with? "acks."
      end

      @log.debug { "Loading #{ack_files} ack files" }
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
        @log.debug { "Loaded #{count}/#{ack_files} ack files" } if (count += 1) % 128 == 0
        @deleted[seg] = acked.sort! unless acked.empty?
        Fiber.yield
      end
      @log.debug { "Loaded #{count} ack files" }
    end

    private def delete_orphan_ack_files
      Dir.each_child(@msg_dir) do |f|
        next unless f.starts_with? "acks."
        seg = f[5, 10].to_u32
        unless @segments.has_key?(seg)
          path = File.join(@msg_dir, f)
          @log.warn { "Deleting orphaned ack file: #{path}" }
          File.delete(path)
          @replicator.try &.delete_file(path, WaitGroup.new)
        end
      end
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

    private def open_ack_file(id) : MFile
      path = File.join(@msg_dir, "acks.#{id.to_s.rjust(10, '0')}")
      capacity = Config.instance.segment_size // BytesMessage::MIN_BYTESIZE * 4 + 4
      mfile = MFile.new(path, capacity, writeonly: true)
      mfile.delete unless @durable # mark as deleted if non-durable
      @replicator.try &.register_file mfile
      mfile
    end

    private def delete_file(file : MFile, including_meta = false)
      File.delete?("#{file.path}.meta") if including_meta
      file.delete(raise_on_missing: false)
      if replicator = @replicator
        replicator.delete_file("#{file.path}.meta", WaitGroup.new) if including_meta
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

    private def load_segments_from_disk : Nil
      ids = Array(UInt32).new
      Dir.each_child(@msg_dir) do |f|
        if f.starts_with?("msgs.") && !f.ends_with?(".meta")
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
            @log.warn { "Empty file at #{path}, deleting it" }
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
            @log.error { "Could not initialize segment #{seg}, closing message store: #{ex.message}" }
            close
          end
        end
        file.pos = 4
        @segments[seg] = file
        Fiber.yield
      end
    end
  end
end
