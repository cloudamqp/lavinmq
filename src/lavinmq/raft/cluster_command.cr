module LavinMQ::Raft
  abstract struct ClusterCommand
    SCHEMA_VERSION  = 1_u8
    HEADER_BYTESIZE =    2 # version + tag

    enum Tag : UInt8
      SetSecret = 0
      SetIsr    = 1
    end

    abstract def tag : Tag
    abstract def body_to_io(io : IO, format : IO::ByteFormat) : Nil
    abstract def body_bytesize : Int32

    def to_io(io : IO, format : IO::ByteFormat) : Nil
      io.write_bytes(SCHEMA_VERSION, format)
      io.write_bytes(tag.value, format)
      body_to_io(io, format)
    end

    def bytesize : Int32
      HEADER_BYTESIZE + body_bytesize
    end

    def self.from_io(io : IO, format : IO::ByteFormat) : ClusterCommand
      version = io.read_bytes(UInt8, format)
      raise InvalidSchemaVersion.new(version) unless version == SCHEMA_VERSION
      tag = Tag.new(io.read_bytes(UInt8, format))
      case tag
      in .set_secret? then SetSecret.read_body(io, format)
      in .set_isr?    then SetIsr.read_body(io, format)
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

      def body_to_io(io : IO, format : IO::ByteFormat) : Nil
        io.write_bytes(@secret.bytesize.to_u32, format)
        io.write(@secret.to_slice)
      end

      def body_bytesize : Int32
        4 + @secret.bytesize
      end

      protected def self.read_body(io : IO, format : IO::ByteFormat) : SetSecret
        len = io.read_bytes(UInt32, format)
        buf = Bytes.new(len)
        io.read_fully(buf)
        new(String.new(buf))
      end
    end

    struct SetIsr < ClusterCommand
      getter node_ids : Set(UInt64)

      def initialize(@node_ids : Set(UInt64))
      end

      def tag : Tag
        Tag::SetIsr
      end

      def body_to_io(io : IO, format : IO::ByteFormat) : Nil
        io.write_bytes(@node_ids.size.to_u32, format)
        @node_ids.each { |id| io.write_bytes(id, format) }
      end

      def body_bytesize : Int32
        4 + @node_ids.size * 8
      end

      protected def self.read_body(io : IO, format : IO::ByteFormat) : SetIsr
        count = io.read_bytes(UInt32, format)
        ids = Set(UInt64).new(initial_capacity: count.to_i32)
        count.times { ids.add(io.read_bytes(UInt64, format)) }
        new(ids)
      end
    end
  end
end
