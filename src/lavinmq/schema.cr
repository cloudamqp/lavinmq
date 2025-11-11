require "../stdlib/copy_file_range"

module LavinMQ
  module Schema
    VERSION = 4

    Log = LavinMQ::Log.for "schema"

    def self.migrate(data_dir, replicator) : Nil
      case v = version(data_dir)
      when 4
      when nil # before version 4 there was no schema_version file
        backup_dir = backup(data_dir)
        begin
          SchemaV4.new(data_dir).migrate
          delete_backup(backup_dir)
        rescue ex
          restore(data_dir, backup_dir)
          raise ex
        end
      else
        raise UnsupportedSchemaVersion.new(v, data_dir)
      end
      replicator.try &.replace_file(File.join(data_dir, "schema_version"))
    end

    private def self.version(data_dir) : Int32?
      File.read(File.join(data_dir, "schema_version")).to_i32
    rescue File::NotFoundError
      nil
    end

    private def self.backup(data_dir) : String?
      children = Dir.children(data_dir).reject!(&.in?("backups", ".lock"))
      return if children.empty?

      backup_dir = File.join(data_dir, "backups", Time.utc.to_rfc3339)
      Log.info { "Saving a backup of #{data_dir} to #{backup_dir}" }
      Dir.mkdir_p(backup_dir)
      children.each do |child|
        FileUtils.cp_r File.join(data_dir, child), backup_dir
      end
      backup_dir
    end

    private def self.restore(data_dir, backup_dir)
      return if backup_dir.nil?

      Log.info { "Restoring backup #{backup_dir}" }
      # delete everything in data dir except backups
      Dir.each_child(data_dir) do |child|
        next if child.in?("backups", ".lock")
        FileUtils.rm_r File.join(data_dir, child)
      end
      # move the backup files to data dir
      Dir.each_child(backup_dir) do |c|
        FileUtils.mv File.join(backup_dir, c), File.join(data_dir, c)
      end
    end

    private def self.delete_backup(backup_dir)
      return if backup_dir.nil?

      Log.warn { "Migration successful, backup #{backup_dir} can be deleted" }
    end

    # Migrates a data directory from version 1-3 to version 4
    class SchemaV4
      Log = LavinMQ::Log.for "schema_v4"

      def initialize(@data_dir : String)
      end

      def migrate
        vhosts do |vhost_dir|
          VhostMigrator.new(vhost_dir).migrate
        end
        File.write(File.join(@data_dir, "schema_version"), Schema::VERSION)
      end

      private def vhosts(&)
        File.open(File.join(@data_dir, "vhosts.json")) do |f|
          JSON.parse(f).as_a.each do |vhost|
            dir = vhost["dir"].as_s
            yield File.join(@data_dir, dir)
          end
        end
      rescue File::NotFoundError
        Log.debug { "Can't migrate data directory, vhosts.json is missing" }
      end

      class VhostMigrator
        def initialize(@vhost_dir : String)
        end

        def migrate
          vhost_segments = Hash(UInt32, MFile).new { |h, k| h[k] = MFile.new("#{@vhost_dir}/msgs.#{k.to_s.rjust(10, '0')}") }
          queues.each do |queue|
            queue_dir = File.join(@vhost_dir, Digest::SHA1.hexdigest queue)
            QueueMigrator.new(queue_dir, vhost_segments).migrate
          end
          vhost_segments.each_value &.delete
          vhost_segments.each_value &.close
        end

        private def queues : Array(String)
          queues = Array(String).new
          File.open(File.join(@vhost_dir, "definitions.amqp")) do |io|
            version = io.read_bytes Int32
            raise UnsupportedSchemaVersion.new(version, io.path) unless version == 1

            loop do
              AMQP::Frame.from_io(io, IO::ByteFormat::SystemEndian) do |frame|
                case frame
                when AMQP::Frame::Queue::Declare
                  queues.push frame.queue_name
                when AMQP::Frame::Queue::Delete
                  queues.delete frame.queue_name
                end
              end
            rescue IO::EOFError
              break
            end
          end
          queues
        end

        class QueueMigrator
          def initialize(@queue_dir : String, @vhost_segments : Hash(UInt32, MFile))
          end

          def migrate
            i = 0
            wfile_id = 1u32
            wfile = File.new("#{@queue_dir}/msgs.#{wfile_id.to_s.rjust(10, '0')}", "w").tap &.sync = true
            wfile.write_bytes Schema::VERSION
            enqs do |sp|
              vseg = @vhost_segments[sp.segment]
              vseg.seek sp.position
              IO.copy(vseg, wfile, sp.bytesize) == sp.bytesize || raise IO::EOFError.new
              if wfile.pos >= Config.instance.segment_size
                wfile.close
                wfile_id += 1
                wfile = File.new("#{@queue_dir}/msgs.#{wfile_id.to_s.rjust(10, '0')}", "w").tap &.sync = true
                wfile.write_bytes Schema::VERSION
              end
              i += 1
            end
            wfile.close
            File.delete File.join(@queue_dir, "ack")
            File.delete File.join(@queue_dir, "enq")
            Log.info { "Migrated #{i} messages in #{@queue_dir}" }
          end

          private def enqs(& : SegmentPositionBase -> Nil) : Nil
            acks = acks()
            MFile.open("#{@queue_dir}/enq") do |f|
              segment_position_class = segment_position_class(f)
              loop do
                sp = segment_position_class.from_io f
                if sp.zero?
                  goto_next_block(f) # if holes in index
                elsif acks.bsearch { |asp| asp >= sp } == sp
                  next # already acked
                else
                  yield sp
                end
              rescue IO::EOFError
                break
              end
            end
          end

          private def acks : Array(SegmentPositionBase)
            ack_path = File.join(@queue_dir, "ack")
            acks = Array(SegmentPositionBase).new
            MFile.open(ack_path) do |f|
              segment_position_class = segment_position_class(f)
              loop do
                sp = segment_position_class.from_io f
                if sp.zero?
                  goto_next_block(f) # if holes in index
                else
                  acks << sp
                end
              rescue IO::EOFError
                break
              end
            end
            acks.sort!
          end

          private def goto_next_block(f)
            new_pos = ((f.pos // 4096) + 1) * 4096
            f.pos = new_pos
          end

          private def segment_position_class(f)
            case schema_version = f.read_bytes Int32
            when 1 then SegmentPositionV1
            when 2 then SegmentPositionV2
            when 3 then SegmentPositionV3
            else        raise OutdatedSchemaVersion.new(schema_version, f.path)
            end
          end
        end
      end
    end

    abstract struct SegmentPositionBase
      include Comparable(self)
      getter segment : UInt32
      getter position : UInt32
      getter bytesize : UInt32

      def initialize(@segment : UInt32, @position : UInt32, @bytesize : UInt32)
      end

      def zero?
        segment == position == 0u32
      end

      def <=>(other : self)
        r = segment <=> other.segment
        return r unless r.zero?
        position <=> other.position
      end
    end

    struct SegmentPositionV1 < SegmentPositionBase
      def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
        seg = UInt32.from_io(io, format)
        pos = UInt32.from_io(io, format)
        bytesize = UInt32.from_io(io, format)
        # SegmentPosition at schema version 1 also included:
        # expiration_ts and priority
        # skipping them as we don't need them
        io.skip(sizeof(Int64) + sizeof(UInt8))
        self.new(seg, pos, bytesize)
      end
    end

    struct SegmentPositionV2 < SegmentPositionBase
      def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
        seg = UInt32.from_io(io, format)
        pos = UInt32.from_io(io, format)
        bytesize = UInt32.from_io(io, format)
        # SegmentPosition at schema version 2 also included:
        # expiration_ts and priority, flags
        # skipping them as we don't need them
        io.skip(sizeof(Int64) + sizeof(UInt8) + sizeof(UInt8))
        self.new(seg, pos, bytesize)
      end
    end

    struct SegmentPositionV3 < SegmentPositionBase
      def self.from_io(io : IO, format = IO::ByteFormat::SystemEndian)
        seg = UInt32.from_io(io, format)
        pos = UInt32.from_io(io, format)
        bytesize = UInt32.from_io(io, format)
        # SegmentPosition at schema version 3 also included:
        # expiration_ts, ttl, priority, flags
        # skipping them as we don't need them
        io.skip(sizeof(Int64) + sizeof(Int64) + sizeof(UInt8) + sizeof(UInt8))
        self.new(seg, pos, bytesize)
      end
    end
  end

  class SchemaVersion
    Log = LavinMQ::Log.for "schema_version"

    VERSIONS = {
      definition: 1,
      message:    4,
      index:      4,
    }

    def self.verify(file : File, type) : Int32
      version = file.read_bytes Int32
      if version != VERSIONS[type]
        raise OutdatedSchemaVersion.new version, file.path
      end
      version
    end

    def self.verify(file : MFile, type) : Int32
      buf = uninitialized UInt8[4]
      file.read_at(0, buf.to_slice)
      version = IO::ByteFormat::SystemEndian.decode(Int32, buf.to_slice)
      if version == 0 # if version is 0, read 8 more bytes(ts) and check if that's also 0. If so, the file is empty, set version to default.
        buffer = uninitialized UInt8[8]
        file.read_at(4, buffer.to_slice)
        raise IO::EOFError.new if buf.all?(&.zero?)
      end
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
  end

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
end
