require "uuid"
require "./errors"

module LavinMQ
  module AMQP10
    # AMQP 1.0 symbol (ASCII, distinct from a UTF-8 string on the wire).
    # Wraps a String because Crystal's `::Symbol` can't be built at runtime.
    record Symbol, value : String do
      def to_s(io : IO) : Nil
        io << @value
      end
    end

    # A described type: a descriptor (usually a `Symbol` or `UInt64` code) paired
    # with a value. Performatives and message sections are described types.
    # A class (not a struct) because it is part of the recursive `AnyValue` union.
    class Described
      getter descriptor : Codec::AnyValue
      getter value : Codec::AnyValue

      def initialize(@descriptor : Codec::AnyValue, @value : Codec::AnyValue)
      end

      def_equals_and_hash descriptor, value
    end

    # Encoder/decoder for the AMQP 1.0 type system
    # (OASIS amqp-core-types-v1.0). Covers the subset LavinMQ needs: the
    # primitives, symbols, binary, lists, maps and described types used by the
    # performatives and message sections.
    module Codec
      extend self

      # Every value the codec can read or write. Integers are normalised on
      # decode (unsigned -> UInt64, signed -> Int64) and re-narrowed by callers.
      alias AnyValue = Nil | Bool | Int64 | UInt64 | Float32 | Float64 | String | Bytes | Time | ::UUID | Symbol | Array(AnyValue) | Hash(AnyValue, AnyValue) | Described

      # Format codes (amqp-core-types-v1.0 §1.6)
      NULL       = 0x40_u8
      BOOL       = 0x56_u8
      BOOL_TRUE  = 0x41_u8
      BOOL_FALSE = 0x42_u8
      UBYTE      = 0x50_u8
      USHORT     = 0x60_u8
      UINT       = 0x70_u8
      SMALLUINT  = 0x52_u8
      UINT0      = 0x43_u8
      ULONG      = 0x80_u8
      SMALLULONG = 0x53_u8
      ULONG0     = 0x44_u8
      BYTE       = 0x51_u8
      SHORT      = 0x61_u8
      INT        = 0x71_u8
      SMALLINT   = 0x54_u8
      LONG       = 0x81_u8
      SMALLLONG  = 0x55_u8
      FLOAT      = 0x72_u8
      DOUBLE     = 0x82_u8
      TIMESTAMP  = 0x83_u8
      UUID_CODE  = 0x98_u8
      VBIN8      = 0xa0_u8
      VBIN32     = 0xb0_u8
      STR8       = 0xa1_u8
      STR32      = 0xb1_u8
      SYM8       = 0xa3_u8
      SYM32      = 0xb3_u8
      LIST0      = 0x45_u8
      LIST8      = 0xc0_u8
      LIST32     = 0xd0_u8
      MAP8       = 0xc1_u8
      MAP32      = 0xd1_u8
      ARRAY8     = 0xe0_u8
      ARRAY32    = 0xf0_u8
      DESCRIBED  = 0x00_u8

      BE = IO::ByteFormat::NetworkEndian

      # ---- decoding -------------------------------------------------------

      def read(io : IO) : AnyValue
        code = read_byte(io)
        read_with_code(io, code)
      end

      # Read a value given an already-consumed format code.
      # ameba:disable Metrics/CyclomaticComplexity
      def read_with_code(io : IO, code : UInt8) : AnyValue
        case code
        when NULL       then nil
        when BOOL_TRUE  then true
        when BOOL_FALSE then false
        when BOOL       then read_byte(io) != 0u8
        when UBYTE      then read_byte(io).to_u64
        when USHORT     then UInt16.from_io(io, BE).to_u64
        when UINT       then UInt32.from_io(io, BE).to_u64
        when SMALLUINT  then read_byte(io).to_u64
        when UINT0      then 0_u64
        when ULONG      then UInt64.from_io(io, BE)
        when SMALLULONG then read_byte(io).to_u64
        when ULONG0     then 0_u64
        when BYTE       then read_byte(io).to_i8!.to_i64
        when SHORT      then Int16.from_io(io, BE).to_i64
        when INT        then Int32.from_io(io, BE).to_i64
        when SMALLINT   then read_byte(io).to_i8!.to_i64
        when LONG       then Int64.from_io(io, BE)
        when SMALLLONG  then read_byte(io).to_i8!.to_i64
        when FLOAT      then Float32.from_io(io, BE)
        when DOUBLE     then Float64.from_io(io, BE)
        when TIMESTAMP  then Time.unix_ms(Int64.from_io(io, BE))
        when UUID_CODE  then read_uuid(io)
        when VBIN8      then read_binary(io, read_byte(io).to_i)
        when VBIN32     then read_binary(io, Int32.from_io(io, BE))
        when STR8       then read_string(io, read_byte(io).to_i)
        when STR32      then read_string(io, Int32.from_io(io, BE))
        when SYM8       then Symbol.new(read_string(io, read_byte(io).to_i))
        when SYM32      then Symbol.new(read_string(io, Int32.from_io(io, BE)))
        when LIST0      then [] of AnyValue
        when LIST8      then read_list(io, read_byte(io).to_i, read_byte(io).to_i)
        when LIST32     then read_list(io, Int32.from_io(io, BE), Int32.from_io(io, BE))
        when MAP8       then read_map(io, read_byte(io).to_i, read_byte(io).to_i)
        when MAP32      then read_map(io, Int32.from_io(io, BE), Int32.from_io(io, BE))
        when ARRAY8     then read_array(io, read_byte(io).to_i, read_byte(io).to_i)
        when ARRAY32    then read_array(io, Int32.from_io(io, BE), Int32.from_io(io, BE))
        when DESCRIBED  then read_described(io)
        else                 raise Error::Decode.new("Unknown AMQP 1.0 format code 0x#{code.to_s(16)}")
        end
      end

      private def read_described(io : IO) : Described
        descriptor = read(io)
        value = read(io)
        Described.new(descriptor, value)
      end

      # Read a list as an array of values. `size` is the byte size of the
      # remaining list content (count field + elements); we rely on `count`.
      private def read_list(io : IO, size : Int, count : Int) : Array(AnyValue)
        arr = Array(AnyValue).new(count)
        count.times { arr << read(io) }
        arr
      end

      private def read_map(io : IO, size : Int, count : Int) : Hash(AnyValue, AnyValue)
        hash = Hash(AnyValue, AnyValue).new(initial_capacity: count // 2)
        (count // 2).times do
          k = read(io)
          v = read(io)
          hash[k] = v
        end
        hash
      end

      # Arrays are encoded with a single constructor shared by all elements.
      private def read_array(io : IO, size : Int, count : Int) : Array(AnyValue)
        code = read_byte(io)
        arr = Array(AnyValue).new(count)
        count.times { arr << read_with_code(io, code) }
        arr
      end

      private def read_uuid(io : IO) : UUID
        bytes = uninitialized UInt8[16]
        io.read_fully(bytes.to_slice)
        UUID.new(bytes)
      end

      private def read_binary(io : IO, len : Int) : Bytes
        bytes = Bytes.new(len)
        io.read_fully(bytes)
        bytes
      end

      private def read_string(io : IO, len : Int) : String
        io.read_string(len)
      end

      private def read_byte(io : IO) : UInt8
        io.read_byte || raise Error::Decode.new("Unexpected EOF")
      end

      # ---- encoding -------------------------------------------------------

      # ameba:disable Metrics/CyclomaticComplexity
      def write(io : IO, v : AnyValue) : Nil
        case v
        when Nil    then io.write_byte NULL
        when Bool   then io.write_byte(v ? BOOL_TRUE : BOOL_FALSE)
        when UInt64 then write_ulong(io, v)
        when Int64  then write_long(io, v)
        when Float32
          io.write_byte FLOAT
          v.to_io(io, BE)
        when Float64
          io.write_byte DOUBLE
          v.to_io(io, BE)
        when Time
          io.write_byte TIMESTAMP
          v.to_unix_ms.to_io(io, BE)
        when ::UUID
          io.write_byte UUID_CODE
          io.write v.bytes.to_slice
        when Symbol                   then write_symbol(io, v.value)
        when String                   then write_string(io, v)
        when Bytes                    then write_binary(io, v)
        when Array(AnyValue)          then write_list(io, v)
        when Hash(AnyValue, AnyValue) then write_map(io, v)
        when Described
          io.write_byte DESCRIBED
          write(io, v.descriptor)
          write(io, v.value)
        else raise Error.new("Cannot encode #{v.class}")
        end
      end

      def write_ulong(io : IO, v : UInt64) : Nil
        if v.zero?
          io.write_byte ULONG0
        elsif v <= UInt8::MAX
          io.write_byte SMALLULONG
          io.write_byte v.to_u8
        else
          io.write_byte ULONG
          v.to_io(io, BE)
        end
      end

      def write_long(io : IO, v : Int64) : Nil
        if Int8::MIN <= v <= Int8::MAX
          io.write_byte SMALLLONG
          io.write_byte v.to_i8!.to_u8!
        else
          io.write_byte LONG
          v.to_io(io, BE)
        end
      end

      # uint, used for handles/ids/windows.
      def write_uint(io : IO, v : UInt32) : Nil
        if v.zero?
          io.write_byte UINT0
        elsif v <= UInt8::MAX
          io.write_byte SMALLUINT
          io.write_byte v.to_u8
        else
          io.write_byte UINT
          v.to_io(io, BE)
        end
      end

      def write_ushort(io : IO, v : UInt16) : Nil
        io.write_byte USHORT
        v.to_io(io, BE)
      end

      def write_symbol(io : IO, v : String) : Nil
        if v.bytesize <= UInt8::MAX
          io.write_byte SYM8
          io.write_byte v.bytesize.to_u8
        else
          io.write_byte SYM32
          v.bytesize.to_u32.to_io(io, BE)
        end
        io.write v.to_slice
      end

      def write_string(io : IO, v : String) : Nil
        if v.bytesize <= UInt8::MAX
          io.write_byte STR8
          io.write_byte v.bytesize.to_u8
        else
          io.write_byte STR32
          v.bytesize.to_u32.to_io(io, BE)
        end
        io.write v.to_slice
      end

      def write_binary(io : IO, v : Bytes) : Nil
        if v.bytesize <= UInt8::MAX
          io.write_byte VBIN8
          io.write_byte v.bytesize.to_u8
        else
          io.write_byte VBIN32
          v.bytesize.to_u32.to_io(io, BE)
        end
        io.write v
      end

      def write_list(io : IO, v : Array(AnyValue)) : Nil
        if v.empty?
          io.write_byte LIST0
          return
        end
        # Encode elements to a temp buffer to know the byte size.
        buf = IO::Memory.new
        v.each { |e| write(io: buf, v: e) }
        write_compound(io, LIST8, LIST32, buf, v.size)
      end

      def write_map(io : IO, v : Hash(AnyValue, AnyValue)) : Nil
        buf = IO::Memory.new
        v.each do |k, val|
          write(buf, k)
          write(buf, val)
        end
        write_compound(io, MAP8, MAP32, buf, v.size * 2)
      end

      def write_ubyte(io : IO, v : UInt8) : Nil
        io.write_byte UBYTE
        io.write_byte v
      end

      # Encoding of a single null field, reused when building composites.
      NULL_BYTES = Bytes[NULL]

      # Write a described list (a "composite type": descriptor + a list of
      # pre-encoded fields), as used by every performative and message section.
      # Trailing null fields are dropped — decoders default missing fields to
      # null (amqp-core-types-v1.0 §1.4.2.3).
      def write_described_list(io : IO, descriptor : UInt64, fields : Array(Bytes)) : Nil
        last = fields.size
        while last > 0 && fields[last - 1].size == 1 && fields[last - 1][0] == NULL
          last -= 1
        end
        io.write_byte DESCRIBED
        write_ulong(io, descriptor)
        if last.zero?
          io.write_byte LIST0
          return
        end
        body = IO::Memory.new
        last.times { |i| body.write(fields[i]) }
        write_compound(io, LIST8, LIST32, body, last)
      end

      # Write a list/map body: choose the 8- or 32-bit width based on size.
      private def write_compound(io : IO, code8 : UInt8, code32 : UInt8, buf : IO::Memory, count : Int) : Nil
        body = buf.to_slice
        # size field counts the count field + the element bytes
        if body.bytesize + 1 <= UInt8::MAX && count <= UInt8::MAX
          io.write_byte code8
          io.write_byte (body.bytesize + 1).to_u8
          io.write_byte count.to_u8
        else
          io.write_byte code32
          (body.bytesize + 4).to_u32.to_io(io, BE)
          count.to_u32.to_io(io, BE)
        end
        io.write body
      end
    end
  end
end
