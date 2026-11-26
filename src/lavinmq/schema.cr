require "json"
require "./filesystem"

module LavinMQ
  module Schema
    VERSION = 4

    Log = LavinMQ::Log.for "schema"

    # Data directories written before LavinMQ 1.0 had no schema_version file
    # and have to be migrated by LavinMQ 2.11 or earlier first. A later data
    # directory can have lost the file, or have it empty, after a power loss:
    # it's written again.
    def self.verify(data_dir, replicator) : Nil
      path = File.join(data_dir, "schema_version")
      case v = version(path)
      when VERSION
      when nil
        vhosts_path = File.join(data_dir, "vhosts.json")
        if File.exists?(vhosts_path)
          case pre_v4_queues(data_dir, vhosts_path)
          in .all?  then raise UnsupportedSchemaVersion.new(path)
          in .some? then raise UnsupportedSchemaVersion.new(path, migration_interrupted: true)
          in .none?
          end
          Log.warn { "#{path} was missing or empty in a data directory with the current layout, writing it again" }
        end
        # Written durably: a confirm doesn't sync the whole data dir, so
        # nothing else makes this file durable, and a power loss could
        # leave it empty
        FileSystem.replace(path, &.print(VERSION))
      else
        raise UnsupportedSchemaVersion.new(v, path)
      end
      replicator.try &.replace_file(path)
    end

    private def self.version(path) : Int32?
      content = File.read(path).strip
      return if content.empty?
      content.to_i32? || raise UnsupportedSchemaVersion.new(content, path)
    rescue File::NotFoundError
      nil
    end

    private enum PreV4Queues
      None
      Some
      All
    end

    # Before schema version 4 every queue directory had an enq and an ack
    # index, and the messages were in the vhost directory. The 2.11
    # migration moves the messages of one queue at a time into its directory.
    private def self.pre_v4_queues(data_dir, vhosts_path) : PreV4Queues
      pre_v4 = migrated = false
      File.open(vhosts_path) do |f|
        JSON.parse(f).as_a.each do |vhost|
          vhost_dir = File.join(data_dir, vhost["dir"].as_s)
          next unless Dir.exists?(vhost_dir)
          Dir.each_child(vhost_dir) do |queue|
            queue_dir = File.join(vhost_dir, queue)
            next unless File.directory?(queue_dir)
            pre_v4 ||= File.exists?(File.join(queue_dir, "enq"))
            migrated ||= Dir.children(queue_dir).any?(&.starts_with?("msgs."))
          end
        end
      end
      return PreV4Queues::None unless pre_v4
      migrated ? PreV4Queues::Some : PreV4Queues::All
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

    def initialize(content : String, path)
      super "Invalid schema version #{content.inspect} in #{path}"
    end

    def initialize(path, *, migration_interrupted = false)
      if migration_interrupted
        super "Missing #{path}, and a migration by LavinMQ 2.11 was interrupted: some queues have the " \
              "message indexes of LavinMQ < 1.0, others are migrated. Restore the data directory from the " \
              "backup the migration saved in backups/, and upgrade it with LavinMQ 2.11 again."
      else
        super "Missing #{path}, and the data directory has the message indexes of LavinMQ < 1.0. " \
              "Upgrade it with LavinMQ 2.11 before upgrading to this version."
      end
    end
  end
end
