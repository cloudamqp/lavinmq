module LavinMQ
  module MessageStoreMetadata
    private def write_metadata_file(seg : UInt32, wfile : MFile)
      @log.debug { "Write message segment meta file #{wfile.path}.meta" }
      File.open("#{wfile.path}.meta", "w") do |f|
        f.buffer_size = 4096
        write_metadata(f, seg)
      end
      @replicator.try &.replace_file "#{wfile.path}.meta"
    end

    private def write_metadata(io, seg)
      io.write_bytes @segment_msg_count[seg]
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
        begin
          read_metadata_file(seg, mfile)
        rescue File::NotFoundError
          produce_metadata(seg, mfile)
          write_metadata_file(seg, mfile) unless seg == @segments.last_key # this segment is not full yet
        end

        if is_long_queue
          @log.info { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
        else
          @log.debug { "Loaded #{counter}/#{@segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
        end
      end
      @log.info { "Loaded #{counter} segments, #{@size} messages" }
    end

    private def read_metadata_file(seg, mfile)
      count = File.open("#{mfile.path}.meta", &.read_bytes(UInt32))
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

    private def produce_metadata(seg, mfile)
      count = 0u32
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
        @log.error { "Could not initialize segment, closing message store: Failed to read segment #{seg} at pos #{mfile.pos}. #{ex}" }
        close
        return count
      end
      mfile.pos = 4
      mfile.dontneed
      Fiber.yield
      @segment_msg_count[seg] = count
      @log.debug { "Manually counted #{count} msgs from #{mfile.path}" }
    end
  end
end
