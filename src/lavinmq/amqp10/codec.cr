require "./value"
require "./slice_reader"

module LavinMQ::AMQP10
  module Codec
    extend self

    # ameba:disable Metrics/CyclomaticComplexity
    def decode(reader : SliceReader) : Value
      code = reader.read_byte
      case code
      when 0x00
        descriptor = decode(reader)
        value = decode(reader)
        Value.described(descriptor, value)
      when 0x40
        Value.null
      when 0x41
        Value.bool(true)
      when 0x42
        Value.bool(false)
      when 0x43
        Value.uint(0_u32)
      when 0x44
        Value.ulong(0_u64)
      when 0x45
        Value.list(Array(Value).new)
      when 0x50
        Value.ubyte(reader.read_byte)
      when 0x51
        Value.ubyte(reader.read_byte)
      when 0x52
        Value.uint(reader.read_byte.to_u32)
      when 0x53
        Value.ulong(reader.read_byte.to_u64)
      when 0x54
        Value.int(reader.read_byte.to_i8.to_i32)
      when 0x55
        Value.long(reader.read_byte.to_i8.to_i64)
      when 0x56
        Value.bool(!reader.read_byte.zero?)
      when 0x60
        Value.ushort(reader.read_u16)
      when 0x61
        Value.int(reader.read_i16.to_i32)
      when 0x70
        Value.uint(reader.read_u32)
      when 0x71
        Value.int(reader.read_i32)
      when 0x72
        Value.float(reader.read_f32)
      when 0x80
        Value.ulong(reader.read_u64)
      when 0x81
        Value.long(reader.read_i64)
      when 0x82
        Value.double(reader.read_f64)
      when 0x83
        Value.timestamp(reader.read_i64)
      when 0xa0
        size = reader.read_byte.to_i
        Value.binary(reader.read_slice(size))
      when 0xb0
        size = reader.read_u32.to_i
        Value.binary(reader.read_slice(size))
      when 0xa1
        size = reader.read_byte.to_i
        Value.string(String.new(reader.read_slice(size)))
      when 0xb1
        size = reader.read_u32.to_i
        Value.string(String.new(reader.read_slice(size)))
      when 0xa3
        size = reader.read_byte.to_i
        Value.symbol(String.new(reader.read_slice(size)))
      when 0xb3
        size = reader.read_u32.to_i
        Value.symbol(String.new(reader.read_slice(size)))
      when 0xc0
        decode_list8(reader)
      when 0xd0
        decode_list32(reader)
      when 0xc1
        decode_map8(reader)
      when 0xd1
        decode_map32(reader)
      when 0xe0
        decode_array8(reader)
      when 0xf0
        decode_array32(reader)
      else
        raise DecodeError.new("unsupported AMQP 1.0 type code 0x#{code.to_s(16)}")
      end
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 value", cause: ex)
    end

    private def decode_list8(reader)
      size = reader.read_byte.to_i
      count = reader.read_byte.to_i
      list_end = reader.pos + size - 1
      values = Array(Value).new(count)
      count.times { values << decode(reader) }
      reader.skip(list_end - reader.pos) if reader.pos < list_end
      Value.list(values)
    end

    private def decode_list32(reader)
      size = reader.read_u32.to_i
      count = reader.read_u32.to_i
      list_end = reader.pos + size - 4
      values = Array(Value).new(count)
      count.times { values << decode(reader) }
      reader.skip(list_end - reader.pos) if reader.pos < list_end
      Value.list(values)
    end

    private def decode_map8(reader)
      size = reader.read_byte.to_i
      count = reader.read_byte.to_i
      map_end = reader.pos + size - 1
      pairs = Array(Tuple(Value, Value)).new(count // 2)
      (count // 2).times do
        pairs << {decode(reader), decode(reader)}
      end
      reader.skip(map_end - reader.pos) if reader.pos < map_end
      Value.map(pairs)
    end

    private def decode_map32(reader)
      size = reader.read_u32.to_i
      count = reader.read_u32.to_i
      map_end = reader.pos + size - 4
      pairs = Array(Tuple(Value, Value)).new(count // 2)
      (count // 2).times do
        pairs << {decode(reader), decode(reader)}
      end
      reader.skip(map_end - reader.pos) if reader.pos < map_end
      Value.map(pairs)
    end

    private def decode_array8(reader)
      size = reader.read_byte.to_i
      count = reader.read_byte.to_i
      array_end = reader.pos + size - 1
      constructor = reader.read_byte
      values = Array(Value).new(count)
      count.times { values << decode_array_item(reader, constructor) }
      reader.skip(array_end - reader.pos) if reader.pos < array_end
      Value.array(values)
    end

    private def decode_array32(reader)
      size = reader.read_u32.to_i
      count = reader.read_u32.to_i
      array_end = reader.pos + size - 4
      constructor = reader.read_byte
      values = Array(Value).new(count)
      count.times { values << decode_array_item(reader, constructor) }
      reader.skip(array_end - reader.pos) if reader.pos < array_end
      Value.array(values)
    end

    private def decode_array_item(reader, constructor : UInt8) : Value
      case constructor
      when 0xa3
        size = reader.read_byte.to_i
        Value.symbol(String.new(reader.read_slice(size)))
      when 0xb3
        size = reader.read_u32.to_i
        Value.symbol(String.new(reader.read_slice(size)))
      when 0x70
        Value.uint(reader.read_u32)
      when 0x80
        Value.ulong(reader.read_u64)
      else
        raise DecodeError.new("unsupported AMQP 1.0 array constructor 0x#{constructor.to_s(16)}")
      end
    end

    def write_value(io : IO, value : Value) : Nil
      case value.kind
      in .null?
        io.write_byte 0x40_u8
      in .bool?
        io.write_byte(value.bool? ? 0x41_u8 : 0x42_u8)
      in .u_byte?
        io.write_byte 0x50_u8
        io.write_byte value.uint_value.to_u8
      in .u_short?
        io.write_byte 0x60_u8
        write_u16(io, value.uint_value.to_u16)
      in .u_int?
        write_uint(io, value.uint_value)
      in .u_long?
        write_ulong(io, value.uint_value)
      in .int?
        write_int(io, value.int_value)
      in .long?
        write_long(io, value.int_value)
      in .float?
        io.write_byte 0x72_u8
        write_f32(io, value.float_value.to_f32)
      in .double?
        io.write_byte 0x82_u8
        write_f64(io, value.float_value)
      in .timestamp?
        io.write_byte 0x83_u8
        write_i64(io, value.timestamp_value)
      in .binary?
        write_binary(io, value.binary_value)
      in .string?
        write_string(io, value.string_value)
      in .symbol?
        write_symbol(io, value.string_value)
      in .list?
        write_list(io, value.list_value)
      in .map?
        write_map(io, value.map_value)
      in .array?
        write_array(io, value.list_value)
      in .described?
        described = value.described_value
        io.write_byte 0x00_u8
        write_value(io, described.descriptor)
        write_value(io, described.value)
      end
    end

    def encoded_size(value : Value) : Int32
      CounterIO.count { |io| write_value(io, value) }
    end

    def write_described_list(io : IO, code : UInt64, fields : Array(Value)) : Nil
      io.write_byte 0x00_u8
      write_ulong(io, code)
      write_list(io, fields)
    end

    def described_list_size(code : UInt64, fields : Array(Value)) : Int32
      CounterIO.count { |io| write_described_list(io, code, fields) }
    end

    def write_u16(io : IO, value : UInt16) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_u32(io : IO, value : UInt32) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_i32(io : IO, value : Int32) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_i16(io : IO, value : Int16) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_u64(io : IO, value : UInt64) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_i64(io : IO, value : Int64) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_f32(io : IO, value : Float32) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_f64(io : IO, value : Float64) : Nil
      io.write_bytes value, IO::ByteFormat::NetworkEndian
    end

    def write_uint(io : IO, value : UInt64) : Nil
      if value.zero?
        io.write_byte 0x43_u8
      elsif value <= UInt8::MAX
        io.write_byte 0x52_u8
        io.write_byte value.to_u8
      else
        io.write_byte 0x70_u8
        write_u32(io, value.to_u32)
      end
    end

    def write_ulong(io : IO, value : UInt64) : Nil
      if value.zero?
        io.write_byte 0x44_u8
      elsif value <= UInt8::MAX
        io.write_byte 0x53_u8
        io.write_byte value.to_u8
      else
        io.write_byte 0x80_u8
        write_u64(io, value)
      end
    end

    def write_int(io : IO, value : Int64) : Nil
      if Int8::MIN <= value <= Int8::MAX
        io.write_byte 0x54_u8
        io.write_byte value.to_i8.to_u8!
      else
        io.write_byte 0x71_u8
        write_i32(io, value.to_i32)
      end
    end

    def write_long(io : IO, value : Int64) : Nil
      if Int8::MIN <= value <= Int8::MAX
        io.write_byte 0x55_u8
        io.write_byte value.to_i8.to_u8!
      else
        io.write_byte 0x81_u8
        write_i64(io, value)
      end
    end

    def write_bool(io : IO, value : Bool) : Nil
      io.write_byte(value ? 0x41_u8 : 0x42_u8)
    end

    def write_binary(io : IO, value : Bytes) : Nil
      if value.bytesize <= UInt8::MAX
        io.write_byte 0xa0_u8
        io.write_byte value.bytesize.to_u8
      else
        io.write_byte 0xb0_u8
        write_u32(io, value.bytesize.to_u32)
      end
      io.write value
    end

    def write_string(io : IO, value : String) : Nil
      if value.bytesize <= UInt8::MAX
        io.write_byte 0xa1_u8
        io.write_byte value.bytesize.to_u8
      else
        io.write_byte 0xb1_u8
        write_u32(io, value.bytesize.to_u32)
      end
      io << value
    end

    def write_symbol(io : IO, value : String) : Nil
      if value.bytesize <= UInt8::MAX
        io.write_byte 0xa3_u8
        io.write_byte value.bytesize.to_u8
      else
        io.write_byte 0xb3_u8
        write_u32(io, value.bytesize.to_u32)
      end
      io << value
    end

    def write_list(io : IO, values : Array(Value)) : Nil
      if values.empty?
        io.write_byte 0x45_u8
        return
      end

      payload = values.sum(0) { |v| encoded_size(v) }
      if payload + 1 <= UInt8::MAX
        io.write_byte 0xc0_u8
        io.write_byte((payload + 1).to_u8)
        io.write_byte values.size.to_u8
      else
        io.write_byte 0xd0_u8
        write_u32(io, (payload + 4).to_u32)
        write_u32(io, values.size.to_u32)
      end
      values.each { |v| write_value(io, v) }
    end

    def write_map(io : IO, pairs : Array(Tuple(Value, Value))) : Nil
      payload = pairs.sum(0) { |k, v| encoded_size(k) + encoded_size(v) }
      count = pairs.size * 2
      if payload + 1 <= UInt8::MAX && count <= UInt8::MAX
        io.write_byte 0xc1_u8
        io.write_byte((payload + 1).to_u8)
        io.write_byte count.to_u8
      else
        io.write_byte 0xd1_u8
        write_u32(io, (payload + 4).to_u32)
        write_u32(io, count.to_u32)
      end
      pairs.each do |key, value|
        write_value(io, key)
        write_value(io, value)
      end
    end

    def write_array(io : IO, values : Array(Value)) : Nil
      if values.empty?
        io.write_byte 0xe0_u8
        io.write_byte 1_u8
        io.write_byte 0_u8
        return
      end

      if values.all?(&.kind.symbol?)
        payload = 1 + values.sum(0) { |v| 1 + v.string_value.bytesize }
        io.write_byte 0xe0_u8
        io.write_byte payload.to_u8
        io.write_byte values.size.to_u8
        io.write_byte 0xa3_u8
        values.each do |v|
          sym = v.string_value
          io.write_byte sym.bytesize.to_u8
          io << sym
        end
      else
        raise Error.new("unsupported AMQP 1.0 array value")
      end
    end

    class CounterIO < IO
      getter count = 0

      def self.count(&)
        io = new
        yield io
        io.count
      end

      def read(slice : Bytes) : Int32
        0
      end

      def write(slice : Bytes) : Nil
        @count += slice.bytesize
      end

      def write_byte(byte : UInt8) : Nil
        @count += 1
      end
    end
  end
end
