module LavinMQ::Raft
  abstract struct ClusterCommand
    SCHEMA_VERSION = 1_u8

    enum Tag : UInt8
      SetSecret     = 0
      AddToIsr      = 1
      RemoveFromIsr = 2
      # 3 reserved for Cutover (phase 3)
    end

    abstract def tag : Tag
    abstract def to_io(io : IO, format : IO::ByteFormat) : Nil
    abstract def bytesize : Int32

    def self.from_io(io : IO, format : IO::ByteFormat) : ClusterCommand
      version = io.read_bytes(UInt8, format)
      raise InvalidSchemaVersion.new(version) unless version == SCHEMA_VERSION
      tag = Tag.new(io.read_bytes(UInt8, format))
      case tag
      in .set_secret?      then SetSecret.read_body(io, format)
      in .add_to_isr?      then raise NotImplementedError.new("AddToIsr not yet implemented")
      in .remove_from_isr? then raise NotImplementedError.new("RemoveFromIsr not yet implemented")
      end
    end

    class InvalidSchemaVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterCommand schema version: #{version}")
      end
    end

    struct SetSecret < ClusterCommand
      getter secret : String

      def initialize(@secret : String)
      end

      def tag : Tag
        Tag::SetSecret
      end

      def to_io(io : IO, format : IO::ByteFormat) : Nil
        io.write_bytes(SCHEMA_VERSION, format)
        io.write_bytes(tag.value, format)
        io.write_bytes(@secret.bytesize.to_u32, format)
        io.write(@secret.to_slice)
      end

      def bytesize : Int32
        1 + 1 + 4 + @secret.bytesize
      end

      protected def self.read_body(io : IO, format : IO::ByteFormat) : SetSecret
        len = io.read_bytes(UInt32, format)
        buf = Bytes.new(len)
        io.read_fully(buf)
        new(String.new(buf))
      end
    end
  end
end
