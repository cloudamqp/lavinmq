module LavinMQ
  module Schema
    VERSION = 4

    # Data directories written before LavinMQ 1.0 had no schema_version file
    # and have to be migrated by LavinMQ 2.11 or earlier first.
    def self.verify(data_dir, replicator) : Nil
      path = File.join(data_dir, "schema_version")
      case v = version(path)
      when VERSION
      when nil
        if File.exists?(File.join(data_dir, "vhosts.json"))
          raise UnsupportedSchemaVersion.new(path)
        end
        File.write(path, VERSION)
      else
        raise UnsupportedSchemaVersion.new(v, path)
      end
      replicator.try &.replace_file(path)
    end

    private def self.version(path) : Int32?
      File.read(path).to_i32
    rescue File::NotFoundError
      nil
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
        raise IO::EOFError.new if len != 8 || buf.all?(&.zero?)
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

    def initialize(path)
      super "Missing #{path}, the data directory was created by LavinMQ < 1.0. " \
            "Upgrade it with LavinMQ 2.11 before upgrading to this version."
    end
  end
end
