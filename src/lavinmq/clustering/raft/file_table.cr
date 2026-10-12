require "digest/crc32"
require "./ops"
require "../../schema"

module LavinMQ::Clustering::Raft
  # The broker's files as the Raft log describes them, by id: where each is
  # and how large. Paths are relative to the data dir. A staged file (see
  # Ops::Type::Create) has its staging path until it's installed.
  class FileTable
    STAGING_DIR = ".raft_staging"
    PREFIX      = Schema::RAFT_FILES_PREFIX
    MAGIC       = "LMQRFT1"
    Format      = IO::ByteFormat::LittleEndian

    class CorruptError < Exception; end

    class FileEntry
      property path : String
      property size : Int64
      property flags : UInt8
      getter id : UInt32
      # The file's inode on this node, 0 until looked up, not saved
      property ino = 0u64

      def initialize(@id : UInt32, @path : String, @size : Int64 = 0i64, @flags : UInt8 = 0u8)
      end

      def staging? : Bool
        @flags & Ops::STAGING != 0
      end

      def dir? : Bool
        @flags & Ops::DIR != 0
      end

      def dup : FileEntry
        FileEntry.new(@id, @path, @size, @flags)
      end
    end

    @by_id = Hash(UInt32, FileEntry).new
    @by_path = Hash(String, FileEntry).new
    # The next id to give a file
    property next_id = 1u32
    # The highest trash id that has been purged, see FileApplier#reconcile
    property trash_watermark = 0i64

    def self.staging_path(id : UInt32) : String
      File.join(STAGING_DIR, id.to_s)
    end

    def [](id : UInt32) : FileEntry
      @by_id[id]? || raise CorruptError.new("Unknown file id #{id}")
    end

    def []?(id : UInt32) : FileEntry?
      @by_id[id]?
    end

    def by_path?(path : String) : FileEntry?
      @by_path[path]?
    end

    def size : Int32
      @by_id.size
    end

    def empty? : Bool
      @by_id.empty?
    end

    def each(& : FileEntry ->) : Nil
      @by_id.each_value { |e| yield e }
    end

    def add(entry : FileEntry) : Nil
      @by_id[entry.id] = entry
      @by_path[entry.path] = entry
      @next_id = entry.id + 1 if entry.id >= @next_id
    end

    def remove(id : UInt32) : FileEntry?
      entry = @by_id.delete(id) || return
      @by_path.delete(entry.path) if @by_path[entry.path]?.same?(entry)
      entry
    end

    # Gives a file another path, as when a staged file is installed
    def move(entry : FileEntry, path : String) : Nil
      @by_path.delete(entry.path) if @by_path[entry.path]?.same?(entry)
      entry.path = path
      @by_path[path] = entry
    end

    # Files under *dir*, or *dir* itself
    def under(dir : String) : Array(FileEntry)
      prefix = dir.ends_with?('/') ? dir : "#{dir}/"
      @by_id.values.select! { |e| e.path == dir || e.path.starts_with?(prefix) }
    end

    def dup : FileTable
      copy = FileTable.new
      @by_id.each_value { |e| copy.add(e.dup) }
      copy.next_id = @next_id
      copy.trash_watermark = @trash_watermark
      copy
    end

    def ==(other : FileTable) : Bool
      return false unless @next_id == other.next_id && size == other.size
      @by_id.all? do |id, e|
        o = other[id]? || return false
        o.path == e.path && o.size == e.size && o.flags == e.flags
      end
    end

    # Writes the table as of log index *index*: a temp file, fsynced and
    # renamed, then the directory fsynced
    def save(dir : String, index : Int64) : Nil
      io = IO::Memory.new
      io.write MAGIC.to_slice
      io.write_bytes index, Format
      io.write_bytes @trash_watermark, Format
      io.write_bytes @next_id, Format
      io.write_bytes @by_id.size, Format
      @by_id.each_value do |e|
        io.write_bytes e.id, Format
        io.write_byte e.flags
        io.write_bytes e.size, Format
        io.write_bytes e.path.bytesize.to_u16, Format
        io.write e.path.to_slice
      end
      io.write_bytes Digest::CRC32.checksum(io.to_slice), Format
      path = FileTable.path(dir, index)
      tmp = "#{path}.tmp"
      File.open(tmp, "w") do |f|
        f.write io.to_slice
        f.fsync
      end
      File.rename(tmp, path)
      File.open(dir, &.fsync)
    end

    def self.path(dir : String, index : Int64) : String
      File.join(dir, "#{PREFIX}#{index}")
    end

    # The table as of log index *index*, an empty one when there's none and
    # the index is 0, before anything was compacted
    def self.load(dir : String, index : Int64) : FileTable
      bytes = File.open(path(dir, index), &.getb_to_end)
      raise CorruptError.new("#{path(dir, index)} is truncated") if bytes.size < MAGIC.bytesize + 4
      body = bytes[0, bytes.size - 4]
      unless Digest::CRC32.checksum(body) == Format.decode(UInt32, bytes[bytes.size - 4, 4])
        raise CorruptError.new("#{path(dir, index)} checksum mismatch")
      end
      io = IO::Memory.new(body, writable: false)
      raise CorruptError.new("#{path(dir, index)} has an invalid header") unless io.read_string(MAGIC.bytesize) == MAGIC
      raise CorruptError.new("#{path(dir, index)} is for index #{index}") unless io.read_bytes(Int64, Format) == index
      table = FileTable.new
      table.trash_watermark = io.read_bytes Int64, Format
      next_id = io.read_bytes UInt32, Format
      count = io.read_bytes Int32, Format
      count.times do
        id = io.read_bytes UInt32, Format
        flags = io.read_byte || raise IO::EOFError.new
        size = io.read_bytes Int64, Format
        len = io.read_bytes UInt16, Format
        table.add FileEntry.new(id, io.read_string(len), size, flags)
      end
      table.next_id = next_id
      table
    rescue File::NotFoundError
      return FileTable.new if index.zero?
      raise CorruptError.new("The raft state is at #{index}, but #{path(dir, index)} is missing")
    rescue ex : IO::EOFError
      raise CorruptError.new("#{path(dir, index)} is truncated", cause: ex)
    end

    def self.file?(name : String) : Bool
      name.starts_with?(PREFIX)
    end
  end
end
