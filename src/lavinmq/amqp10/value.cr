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

  struct Value
    alias List = Array(Value)
    alias Map = Array(Tuple(Value, Value))
    alias Payload = Nil | Bool | UInt64 | Int64 | Float64 | String | Bytes | List | Map | Described

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

    @payload : Payload

    private def initialize(@kind : Kind, @payload : Payload = nil)
    end

    def self.null
      new(Kind::Null)
    end

    def self.bool(value : Bool)
      new(Kind::Bool, value)
    end

    def self.ubyte(value : UInt8)
      new(Kind::UByte, value.to_u64)
    end

    def self.ushort(value : UInt16)
      new(Kind::UShort, value.to_u64)
    end

    def self.uint(value : UInt32 | UInt64 | Int32 | Int64)
      new(Kind::UInt, value.to_u64)
    end

    def self.ulong(value : UInt64 | UInt32 | Int32 | Int64)
      new(Kind::ULong, value.to_u64)
    end

    def self.int(value : Int32 | Int64)
      new(Kind::Int, value.to_i64)
    end

    def self.long(value : Int64 | Int32)
      new(Kind::Long, value.to_i64)
    end

    def self.float(value : Float32)
      new(Kind::Float, value.to_f64)
    end

    def self.double(value : Float64 | Float32)
      new(Kind::Double, value.to_f64)
    end

    def self.timestamp(value : Int64)
      new(Kind::Timestamp, value)
    end

    def self.binary(value : Bytes)
      new(Kind::Binary, value)
    end

    def self.string(value : String)
      new(Kind::String, value)
    end

    def self.symbol(value : String)
      new(Kind::Symbol, value)
    end

    def self.list(value : List)
      new(Kind::List, value)
    end

    def self.map(value : Map)
      new(Kind::Map, value)
    end

    def self.array(value : List)
      new(Kind::Array, value)
    end

    def self.described(descriptor : Value, value : Value)
      new(Kind::Described, Described.new(descriptor, value))
    end

    def null? : Bool
      @kind.null?
    end

    def bool? : Bool?
      @payload.as(Bool) if @kind.bool?
    end

    def uint? : UInt64?
      @payload.as(UInt64) if @kind.u_byte? || @kind.u_short? || @kind.u_int? || @kind.u_long?
    end

    def uint_value : UInt64
      @payload.as(UInt64)
    end

    def int? : Int64?
      @payload.as(Int64) if @kind.int? || @kind.long? || @kind.timestamp?
    end

    def int_value : Int64
      @payload.as(Int64)
    end

    def float? : Float32?
      @payload.as(Float64).to_f32 if @kind.float?
    end

    def double? : Float64?
      @payload.as(Float64) if @kind.double?
    end

    def float_value : Float64
      @payload.as(Float64)
    end

    def timestamp? : Int64?
      @payload.as(Int64) if @kind.timestamp?
    end

    def timestamp_value : Int64
      @payload.as(Int64)
    end

    def binary? : Bytes?
      @payload.as(Bytes) if @kind.binary?
    end

    def binary_value : Bytes
      @payload.as(Bytes)
    end

    def string? : String?
      @payload.as(String) if @kind.string?
    end

    def string_value : String
      @payload.as(String)
    end

    def symbol? : String?
      @payload.as(String) if @kind.symbol?
    end

    def string_like? : String?
      @payload.as(String) if @kind.string? || @kind.symbol?
    end

    def list? : List?
      @payload.as(List) if @kind.list? || @kind.array?
    end

    def list_value : List
      @payload.as(List)
    end

    def map? : Map?
      @payload.as(Map) if @kind.map?
    end

    def map_value : Map
      @payload.as(Map)
    end

    def described? : Described?
      @payload.as(Described) if @kind.described?
    end

    def described_value : Described
      if described = described?
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
        io << @payload.as(Bool)
      in .u_byte?, .u_short?, .u_int?, .u_long?
        io << @payload.as(UInt64)
      in .int?, .long?, .timestamp?
        io << @payload.as(Int64)
      in .float?, .double?
        io << @payload.as(Float64)
      in .binary?
        io << @payload.as(Bytes)
      in .string?, .symbol?
        io << @payload.as(String).inspect
      in .list?, .array?
        @payload.as(List).inspect(io)
      in .map?
        @payload.as(Map).inspect(io)
      in .described?
        @payload.as(Described).inspect(io)
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
