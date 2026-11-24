require "json"
require "digest/sha1"
require "./filesystem"

module LavinMQ
  module Schema
    VERSION = 4

    Log = LavinMQ::Log.for "schema"

    # LavinMQ < 2.12 wrote schema_version once, without fsync, so a crash
    # early in a data directory's life or a manual delete can lose it from
    # a 1.0+ data directory. A missing or unreadable schema_version is
    # therefore recreated unless the layout shows the data directory was
    # created by LavinMQ < 1.0, which has to be migrated by LavinMQ 2.11.
    def self.verify(data_dir, replicator) : Nil
      path = File.join(data_dir, "schema_version")
      raw = read(path)
      case v = raw.try &.to_i32?
      when VERSION
      when nil
        layout = Layout.new(data_dir)
        raise UnsupportedSchemaVersion.new(path, layout) unless layout.v4?
        if raw || File.exists?(File.join(data_dir, "vhosts.json"))
          Log.warn { "#{raw ? "Invalid" : "Missing"} #{path}, the data directory has a version #{VERSION} layout, recreating it" }
        end
        # Written durably: a confirm doesn't sync the whole data dir, so
        # nothing else makes this file durable, and a power loss could
        # leave it empty, which stops every later start
        FileSystem.replace(path, &.print(VERSION))
      else
        raise UnsupportedSchemaVersion.new(v, path)
      end
      replicator.try &.replace_file(path)
    end

    private def self.read(path) : String?
      File.read(path)
    rescue File::NotFoundError
      nil
    end

    # Tells a data directory created by LavinMQ < 1.0 from a version 4 one.
    # Before 1.0 messages were stored in segments per vhost
    # (<vhost>/msgs.N) and queues indexed them in <vhost>/<queue>/enq and
    # ack files. Since 1.0 each queue has its own segments
    # (<vhost>/<queue>/msgs.N), each starting with the schema version.
    # A data directory without either has no message data, so there is
    # nothing a migration could convert and it's started as version 4.
    struct Layout
      SEGMENT = /\Amsgs\.\d{10}\z/

      getter pre_v1_file : String? = nil
      getter v4_file : String? = nil

      def initialize(data_dir : String)
        vhost_dirs(data_dir).each { |dir| scan_vhost(dir) }
      end

      def v4? : Bool
        @pre_v1_file.nil?
      end

      private def vhost_dirs(data_dir) : Array(String)
        File.open(File.join(data_dir, "vhosts.json")) do |f|
          JSON.parse(f).as_a.map do |vhost|
            dir = vhost["dir"]?.try(&.as_s?) || Digest::SHA1.hexdigest(vhost["name"].as_s)
            File.join(data_dir, dir)
          end
        end
      rescue File::NotFoundError
        Array(String).new
      end

      private def scan_vhost(vhost_dir) : Nil
        Dir.each_child(vhost_dir) do |child|
          path = File.join(vhost_dir, child)
          if SEGMENT.matches?(child)
            @pre_v1_file ||= path
          elsif Dir.exists?(path)
            scan_queue(path)
          end
        end
      rescue File::NotFoundError
      end

      private def scan_queue(queue_dir) : Nil
        Dir.each_child(queue_dir) do |child|
          path = File.join(queue_dir, child)
          if child.in?("enq", "ack")
            @pre_v1_file ||= path
          elsif @v4_file.nil? && SEGMENT.matches?(child) && v4_segment?(path)
            @v4_file = path
          end
        end
      end

      private def v4_segment?(path) : Bool
        File.open(path) { |f| f.read_bytes(Int32) == VERSION }
      rescue IO::EOFError
        false
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
      len = file.read_at(0, buf.to_slice)
      raise IO::EOFError.new if len != 4
      version = IO::ByteFormat::SystemEndian.decode(Int32, buf.to_slice)
      if version == 0 # if version is 0, read 8 more bytes(ts) and check if that's also 0. If so, the file is empty, set version to default.
        buffer = uninitialized UInt8[8]
        len = file.read_at(4, buffer.to_slice)
        raise IO::EOFError.new if len != 8 || buffer.all?(&.zero?)
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
    getter version : Int32?

    def initialize(@version : Int32, path)
      super "Unsupported schema version #{@version} for #{path}"
    end

    def initialize(path, layout : Schema::Layout)
      if pre_v1 = layout.pre_v1_file
        if v4 = layout.v4_file
          super "Missing or invalid #{path}, and the data directory has both files of " \
                "LavinMQ < 1.0 (#{pre_v1}) and of LavinMQ >= 1.0 (#{v4}). " \
                "If LavinMQ >= 1.0 ran on this data directory, the older files are leftovers: " \
                "write #{Schema::VERSION} to #{path}. " \
                "Otherwise upgrade it with LavinMQ 2.11 before upgrading to this version."
        else
          super "Missing or invalid #{path}, the data directory was created by LavinMQ < 1.0 (#{pre_v1}). " \
                "Upgrade it with LavinMQ 2.11 before upgrading to this version."
        end
      else
        super "Missing or invalid #{path}"
      end
    end
  end
end
