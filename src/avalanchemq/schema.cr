module AvalancheMQ
  # Current schema version for files on disk
  SCHEMA_VERSION = 1

  class UnsupportedSchemaVersion < Exception
    def initialize(version : Int32)
      super "Unsupported schema version #{version}"
    end
  end

  class SchemaVersion
    def self.verify(file)
      version = file.read_bytes Int32
      if version != SCHEMA_VERSION
        raise UnsupportedSchemaVersion.new(version)
      end
    end

    def self.prefix(file)
      file.write_bytes SCHEMA_VERSION
      file.flush
    end

    def self.verify_or_prefix(file)
      verify(file)
    rescue IO::EOFError
      prefix(file)
    end
  end
end
