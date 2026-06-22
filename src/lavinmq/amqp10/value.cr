module LavinMQ::AMQP10
  PROTOCOL_HEADER = Bytes['A'.ord.to_u8, 'M'.ord.to_u8, 'Q'.ord.to_u8, 'P'.ord.to_u8, 0_u8, 1_u8, 0_u8, 0_u8]
  SASL_HEADER     = Bytes['A'.ord.to_u8, 'M'.ord.to_u8, 'Q'.ord.to_u8, 'P'.ord.to_u8, 3_u8, 1_u8, 0_u8, 0_u8]

  AMQP_FRAME_TYPE = 0_u8
  SASL_FRAME_TYPE = 1_u8

  MIN_MAX_FRAME_SIZE =    512_u32
  DEFAULT_WINDOW     = 65_535_u32

  class Error < Exception; end

  class DecodeError < Error; end

  class ProtocolError < Error; end

  class Value
    enum Kind
      Null
      Bool
      UByte
      UShort
      UInt
      ULong
      Int
      Long
      Float
      Double
      Timestamp
      Binary
      String
      Symbol
      List
      Map
      Array
      Described
    end

    getter kind

    @bool = false
    @uint = 0_u64
    @int = 0_i64
    @float = 0.0_f64
    @string = ""
    @bytes = Bytes.empty
    @list = Array(Value).new
    @map = Array(Tuple(Value, Value)).new
    @described : Described?

    private def initialize(@kind : Kind,
                           @bool = false,
                           @uint = 0_u64,
                           @int = 0_i64,
                           @float = 0.0_f64,
                           @string = "",
                           @bytes = Bytes.empty,
                           @list = Array(Value).new,
                           @map = Array(Tuple(Value, Value)).new,
                           @described : Described? = nil)
    end

    def self.null
      new(Kind::Null)
    end

    def self.bool(value : Bool)
      new(Kind::Bool, bool: value)
    end

    def self.ubyte(value : UInt8)
      new(Kind::UByte, uint: value.to_u64)
    end

    def self.ushort(value : UInt16)
      new(Kind::UShort, uint: value.to_u64)
    end

    def self.uint(value : UInt32 | UInt64 | Int32 | Int64)
      new(Kind::UInt, uint: value.to_u64)
    end

    def self.ulong(value : UInt64 | UInt32 | Int32 | Int64)
      new(Kind::ULong, uint: value.to_u64)
    end

    def self.int(value : Int32 | Int64)
      new(Kind::Int, int: value.to_i64)
    end

    def self.long(value : Int64 | Int32)
      new(Kind::Long, int: value.to_i64)
    end

    def self.float(value : Float32)
      new(Kind::Float, float: value.to_f64)
    end

    def self.double(value : Float64 | Float32)
      new(Kind::Double, float: value.to_f64)
    end

    def self.timestamp(value : Int64)
      new(Kind::Timestamp, int: value)
    end

    def self.binary(value : Bytes)
      new(Kind::Binary, bytes: value)
    end

    def self.string(value : String)
      new(Kind::String, string: value)
    end

    def self.symbol(value : String)
      new(Kind::Symbol, string: value)
    end

    def self.list(value : Array(Value))
      new(Kind::List, list: value)
    end

    def self.map(value : Array(Tuple(Value, Value)))
      new(Kind::Map, map: value)
    end

    def self.array(value : Array(Value))
      new(Kind::Array, list: value)
    end

    def self.described(descriptor : Value, value : Value)
      new(Kind::Described, described: Described.new(descriptor, value))
    end

    def null? : Bool
      @kind.null?
    end

    def bool? : Bool?
      @bool if @kind.bool?
    end

    def uint? : UInt64?
      @uint if @kind.u_byte? || @kind.u_short? || @kind.u_int? || @kind.u_long?
    end

    def uint_value : UInt64
      @uint
    end

    def int? : Int64?
      @int if @kind.int? || @kind.long? || @kind.timestamp?
    end

    def int_value : Int64
      @int
    end

    def float? : Float32?
      @float.to_f32 if @kind.float?
    end

    def double? : Float64?
      @float if @kind.double?
    end

    def float_value : Float64
      @float
    end

    def timestamp? : Int64?
      @int if @kind.timestamp?
    end

    def timestamp_value : Int64
      @int
    end

    def binary? : Bytes?
      @bytes if @kind.binary?
    end

    def binary_value : Bytes
      @bytes
    end

    def string? : String?
      @string if @kind.string?
    end

    def string_value : String
      @string
    end

    def symbol? : String?
      @string if @kind.symbol?
    end

    def string_like? : String?
      @string if @kind.string? || @kind.symbol?
    end

    def list? : Array(Value)?
      @list if @kind.list? || @kind.array?
    end

    def list_value : Array(Value)
      @list
    end

    def map? : Array(Tuple(Value, Value))?
      @map if @kind.map?
    end

    def map_value : Array(Tuple(Value, Value))
      @map
    end

    def described? : Described?
      @described if @kind.described?
    end

    def described_value : Described
      if described = @described
        described
      else
        raise DecodeError.new("value is not described")
      end
    end

    def descriptor_code? : UInt64?
      described?.try &.descriptor_code?
    end

    def inspect(io : IO) : Nil
      case @kind
      in .null?
        io << "null"
      in .bool?
        io << @bool
      in .u_byte?, .u_short?, .u_int?, .u_long?
        io << @uint
      in .int?, .long?, .timestamp?
        io << @int
      in .float?, .double?
        io << @float
      in .binary?
        io << @bytes
      in .string?, .symbol?
        io << @string.inspect
      in .list?, .array?
        @list.inspect(io)
      in .map?
        @map.inspect(io)
      in .described?
        @described.inspect(io)
      end
    end
  end

  class Described
    getter descriptor, value

    def initialize(@descriptor : Value, @value : Value)
    end

    def descriptor_code? : UInt64?
      @descriptor.uint?
    end
  end
end
