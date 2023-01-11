module LavinMQ
  class OutdatedSchemaVersion < Exception
    getter version : Int32

    def initialize(@version, path)
      super "Outdated schema version #{@version} for #{path}"
    end
  end

  class UnsupportedSchemaVersion < Exception
    getter version : Int32

    def initialize(@version, path)
      super "Cannot migrate #{path} from version #{@version}"
    end
  end

  class SchemaVersion
    Log = ::Log.for("SchemaVersion")

    VERSIONS = {
      definition: 1,
      message:    1,
      index:      3,
    }

    def self.verify(file, type) : Int32
      version = file.read_bytes Int32
      if version != VERSIONS[type]
        raise OutdatedSchemaVersion.new version, file.path
      end
      version
    end

    def self.prefix(file, type) : Int32
      version = VERSIONS[type]
      file.write_bytes version
      file.flush
      version
    end

    def self.verify_or_prefix(file, type)
      verify(file, type)
    rescue IO::EOFError
      prefix(file, type)
    end

    def self.migrate_index_enq(path : String, acked = Array(SegmentPosition).new)
      File.open(path) do |file|
        begin
          self.verify(file, :index)
        rescue ex : OutdatedSchemaVersion
          self.migrate_index_enq(file, ex.version, acked)
        end
      end
    end

    def self.migrate_index_ack(path : String)
      File.open(path) do |file|
        begin
          self.verify(file, :index)
        rescue ex : OutdatedSchemaVersion
          self.migrate_index_ack(file, ex.version)
        end
      end
    end

    def self.migrate_index_enq(file, current_version, acked : Array(SegmentPosition))
      Log.info { "Migrating #{file.path} from version #{current_version} to #{VERSIONS[:index]}" }
      case current_version
      when 1
        MigrateIndex(SegmentPositionV1).run(file, acked)
        return
      when 2
        MigrateIndex(SegmentPositionV2).run(file, acked)
        return
      end
      raise UnsupportedSchemaVersion.new current_version, file.path
    end

    def self.migrate_index_ack(file, current_version)
      Log.info { "Migrating #{file.path} from version #{current_version} to #{VERSIONS[:index]}" }
      case current_version
      when 1
        MigrateIndexAck(SegmentPositionV1).run(file)
        return
      when 2
        MigrateIndexAck(SegmentPositionV2).run(file)
        return
      end
      raise UnsupportedSchemaVersion.new current_version, file.path
    end

    class MigrateIndex(T)
      def self.run(file, acked : Array(SegmentPosition))
        # data dir is one level up from index files
        data_dir = File.join(File.dirname(file.path), "..")
        File.open("#{file.path}.tmp", "w") do |f|
          SchemaVersion.prefix(f, :index)
          prev_segment = 0u32
          seg = nil
          loop do
            sp =
              begin
                T.from_io file
              rescue IO::EOFError
                break
              end
            if sp.zero?
              goto_next_block(file)
              next
            end

            if acked.bsearch { |asp| sp.ref_same_msg?(asp) }
              next
            end

            if prev_segment != sp.segment
              seg.try &.close
              seg = open_segment data_dir, sp.segment
            end
            if segment = seg
              segment.pos = sp.position
              begin
                msg = MessageMetadata.from_io segment
                new_sp = SegmentPosition.make(sp.segment, sp.position, msg)
                f.write_bytes new_sp
              rescue IO::EOFError
                next # if the message has been truncated by GC already
              rescue ex
                Log.error { "sp_seg=#{sp.segment} sp_pos=#{sp.position} current_pos=#{segment.pos}" }
                raise ex
              end
            else
              next # if the file has been deleted by GC already
            end
          end
          seg.try &.close
          File.rename f.path, file.path
          f.fsync
        end
      end

      private def self.open_segment(data_dir, seg)
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        file = File.new(File.join(data_dir, filename))
        file.buffer_size = Config.instance.file_buffer_size
        file
      rescue File::NotFoundError
        nil
      end

      # Jump to the next block in a file
      private def self.goto_next_block(f)
        new_pos = ((f.pos // 4096) + 1) * 4096
        f.pos = new_pos
      end
    end

    class MigrateIndexAck(T)
      def self.run(file, only_read = false)
        # data dir is one level up from index files
        dummy_message = MessageMetadata.new(0, "", "", AMQP::Properties.new, 0)
        File.open("#{file.path}.tmp", "w") do |f|
          SchemaVersion.prefix(f, :index)
          seg = nil
          loop do
            sp =
              begin
                T.from_io file
              rescue IO::EOFError
                break
              end
            if sp.zero?
              goto_next_block(file)
              next
            end

            begin
              new_sp = SegmentPosition.make(sp.segment, sp.position, dummy_message)
              f.write_bytes new_sp
            rescue ex
              Log.error { "sp_seg=#{sp.segment} sp_pos=#{sp.position}" }
              raise ex
            end
          end
          seg.try &.close
          File.rename f.path, file.path
          f.fsync
        end
      end

      private def self.open_segment(data_dir, seg)
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        file = File.new(File.join(data_dir, filename))
        file.buffer_size = Config.instance.file_buffer_size
        file
      rescue File::NotFoundError
        nil
      end

      # Jump to the next block in a file
      private def self.goto_next_block(f)
        new_pos = ((f.pos // 4096) + 1) * 4096
        f.pos = new_pos
      end
    end

    abstract struct SegmentPositionBase
      getter segment : UInt32
      getter position : UInt32

      def initialize(@segment : UInt32, @position : UInt32)
      end

      def zero?
        segment == position == 0u32
      end

      def ref_same_msg?(other : SegmentPosition)
        segment == other.segment && position == other.position
      end
    end

    struct SegmentPositionV1 < SegmentPositionBase
      def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
        seg = UInt32.from_io(io, format)
        pos = UInt32.from_io(io, format)
        # SegmentPosition at schema version 1 also included:
        # bytesize, expiration_ts and priority
        # skipping them as we don't need them
        io.skip(sizeof(UInt32) + sizeof(Int64) + sizeof(UInt8))
        self.new(seg, pos)
      end
    end

    struct SegmentPositionV2 < SegmentPositionBase
      def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
        seg = UInt32.from_io(io, format)
        pos = UInt32.from_io(io, format)
        # SegmentPosition at schema version 2 also included:
        # bytesize, expiration_ts and priority, flags
        # skipping them as we don't need them
        io.skip(sizeof(UInt32) + sizeof(Int64) + sizeof(UInt8) + sizeof(UInt8))
        self.new(seg, pos)
      end
    end
  end
end
