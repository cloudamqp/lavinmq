module LavinMQ::Raft
  abstract struct ClusterCommand
    SCHEMA_VERSION  = 1_u8
    HEADER_BYTESIZE =    2 # version + tag

    # The replication secret is NOT a cluster command: it lives in a local
    # `.clustering_password` file (see Raft::Coordinator#password), never the
    # raft log. Only the ISR is replicated through consensus.
    enum Tag : UInt8
      SetIsr = 0
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
      tag = io.read_bytes(UInt8, format)
      case Tag.from_value?(tag)
      when Tag::SetIsr then SetIsr.read_body(io, format)
      else                  raise InvalidTag.new(tag)
      end
    end

    class InvalidSchemaVersion < Exception
      def initialize(version : UInt8)
        super("Unsupported ClusterCommand schema version: #{version}")
      end
    end

    class InvalidTag < Exception
      def initialize(tag : UInt8)
        super("Unknown ClusterCommand tag: #{tag}")
      end
    end

    struct SetIsr < ClusterCommand
      getter node_ids : Set(Int32)

      def initialize(@node_ids : Set(Int32))
      end

      def tag : Tag
        Tag::SetIsr
      end

      def body_to_io(io : IO, format : IO::ByteFormat) : Nil
        io.write_bytes(@node_ids.size.to_u32, format)
        @node_ids.each { |id| io.write_bytes(id, format) }
      end

      def body_bytesize : Int32
        4 + @node_ids.size * 4
      end

      protected def self.read_body(io : IO, format : IO::ByteFormat) : SetIsr
        count = io.read_bytes(UInt32, format)
        ids = Set(Int32).new(initial_capacity: count.to_i32)
        count.times { ids.add(io.read_bytes(Int32, format)) }
        new(ids)
      end
    end
  end
end
