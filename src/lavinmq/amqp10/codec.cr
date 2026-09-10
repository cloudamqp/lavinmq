require "./value"
require "./io_memory_reset"

module LavinMQ::AMQP10
  module Codec
    extend self

    MAX_DECODE_DEPTH    =     64
    MAX_COMPOUND_VALUES = 65_536

    # Turns IO#read_byte's nilable return into the raising form AMQP10 always
    # wants; not a passthrough over read_bytes, so it earns its keep.
    def read_byte(io : IO) : UInt8
      io.read_byte || raise IO::EOFError.new
    end

    # Zero-copy view into io's own backing buffer, advancing pos past it.
    # Mirrors amq-protocol's Table.from_io IO::Memory fast path (io.@writeable
    # read, not written -- reading a foreign ivar is already used elsewhere in
    # this codebase, e.g. src/lavinmq/mqtt/sessions.cr). Unlike amq-protocol's
    # Table, these readers are short-lived (retired within one synchronous
    # decode call), so -- deliberately, unlike Table.from_io -- we never
    # defensively copy when the source is writeable.
    def read_slice(io : IO::Memory, size : Int) : Bytes
      raise IO::EOFError.new if size < 0 || size > io.bytesize - io.pos
      slice = Bytes.new(io.buffer + io.pos, size, read_only: !io.@writeable)
      io.pos += size
      slice
    end

    # The slice from an earlier saved position up to io's current pos.
    def slice_from(io : IO::Memory, start : Int) : Bytes
      Bytes.new(io.buffer + start, io.pos - start, read_only: !io.@writeable)
    end

    # Reads a 32-bit size field and validates it against the remaining payload,
    # so a hostile oversized size cannot allocate or read past the buffer.
    def read_size32(io : IO::Memory, type : String) : Int32
      size = io.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      if size > (io.bytesize - io.pos).to_u32
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      size.to_i
    end

    def decode(reader : IO::Memory) : Value
      decode(reader, 0)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def decode(reader : IO::Memory, depth : Int32) : Value
      raise DecodeError.new("AMQP 1.0 value nesting too deep") if depth > MAX_DECODE_DEPTH

      code = read_byte(reader)
      case code
      when 0x00
        descriptor = decode(reader, depth + 1)
        value = decode(reader, depth + 1)
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
        Value.ubyte(read_byte(reader))
      when 0x51
        Value.int(read_byte(reader).to_i8!.to_i32)
      when 0x52
        Value.uint(read_byte(reader).to_u32)
      when 0x53
        Value.ulong(read_byte(reader).to_u64)
      when 0x54
        Value.int(read_byte(reader).to_i8!.to_i32)
      when 0x55
        Value.long(read_byte(reader).to_i8!.to_i64)
      when 0x56
        Value.bool(!read_byte(reader).zero?)
      when 0x60
        Value.ushort(reader.read_bytes(UInt16, IO::ByteFormat::NetworkEndian))
      when 0x61
        Value.int(reader.read_bytes(Int16, IO::ByteFormat::NetworkEndian).to_i32)
      when 0x70
        Value.uint(reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian))
      when 0x71
        Value.int(reader.read_bytes(Int32, IO::ByteFormat::NetworkEndian))
      when 0x72
        Value.float(reader.read_bytes(Float32, IO::ByteFormat::NetworkEndian))
      when 0x80
        Value.ulong(reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian))
      when 0x81
        Value.long(reader.read_bytes(Int64, IO::ByteFormat::NetworkEndian))
      when 0x82
        Value.double(reader.read_bytes(Float64, IO::ByteFormat::NetworkEndian))
      when 0x83
        Value.timestamp(reader.read_bytes(Int64, IO::ByteFormat::NetworkEndian))
      when 0xa0
        size = read_byte(reader).to_i
        Value.binary(read_slice(reader, size))
      when 0xb0
        size = read_size32(reader, "binary32")
        Value.binary(read_slice(reader, size))
      when 0xa1
        size = read_byte(reader).to_i
        Value.string(reader.read_string(size))
      when 0xb1
        size = read_size32(reader, "string32")
        Value.string(reader.read_string(size))
      when 0xa3
        size = read_byte(reader).to_i
        Value.symbol(reader.read_string(size))
      when 0xb3
        size = read_size32(reader, "symbol32")
        Value.symbol(reader.read_string(size))
      when 0xc0
        decode_list8(reader, depth)
      when 0xd0
        decode_list32(reader, depth)
      when 0xc1
        decode_map8(reader, depth)
      when 0xd1
        decode_map32(reader, depth)
      when 0xe0
        decode_array8(reader, depth)
      when 0xf0
        decode_array32(reader, depth)
      else
        raise DecodeError.new("unsupported AMQP 1.0 type code 0x#{code.to_s(16)}")
      end
    rescue ex : IO::EOFError
      raise DecodeError.new("truncated AMQP 1.0 value", cause: ex)
    end

    private def decode_list8(reader, depth)
      size = read_byte(reader).to_i
      count = read_byte(reader).to_i
      end_pos = compound_end_pos(reader, "list8", size, 1, count)
      values = Array(Value).new(count)
      count.times { values << decode(reader, depth + 1) }
      raise DecodeError.new("list8 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.list(values)
    end

    private def decode_list32(reader, depth)
      size = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      count = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      end_pos = compound_end_pos(reader, "list32", size, 4, count)
      values = Array(Value).new(count.to_i)
      count.times { values << decode(reader, depth + 1) }
      raise DecodeError.new("list32 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.list(values)
    end

    private def decode_map8(reader, depth)
      size = read_byte(reader).to_i
      count = read_byte(reader).to_i
      end_pos = compound_end_pos(reader, "map8", size, 1, count)
      validate_map_count(count)
      pairs = Array(Tuple(Value, Value)).new(count // 2)
      (count // 2).times do
        pairs << {decode(reader, depth + 1), decode(reader, depth + 1)}
      end
      raise DecodeError.new("map8 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.map(pairs)
    end

    private def decode_map32(reader, depth)
      size = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      count = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      end_pos = compound_end_pos(reader, "map32", size, 4, count)
      validate_map_count(count)
      pairs = Array(Tuple(Value, Value)).new((count // 2).to_i)
      (count // 2).times do
        pairs << {decode(reader, depth + 1), decode(reader, depth + 1)}
      end
      raise DecodeError.new("map32 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.map(pairs)
    end

    private def decode_array8(reader, depth)
      size = read_byte(reader).to_i
      count = read_byte(reader).to_i
      end_pos = array_end_pos(reader, "array8", size, 1, count)
      return Value.array(Array(Value).new) if count.zero?

      constructor = read_byte(reader)
      values = Array(Value).new(count)
      count.times { values << decode_array_item(reader, constructor) }
      raise DecodeError.new("array8 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.array(values)
    end

    private def decode_array32(reader, depth)
      size = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      count = reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian)
      end_pos = array_end_pos(reader, "array32", size, 4, count)
      return Value.array(Array(Value).new) if count.zero?

      constructor = read_byte(reader)
      values = Array(Value).new(count.to_i)
      count.times { values << decode_array_item(reader, constructor) }
      raise DecodeError.new("array32 elements overran declared size") if reader.pos > end_pos
      reader.skip(end_pos - reader.pos) if reader.pos < end_pos
      Value.array(values)
    end

    private def compound_end_pos(reader, type : String, size : Int | UInt32, count_width : Int32, count : Int | UInt32) : Int32
      payload_size = validate_compound_header(reader, type, size, count_width, count)
      if count > payload_size
        raise DecodeError.new("#{type} count #{count} exceeds payload size #{payload_size}")
      end
      reader.pos + payload_size
    end

    private def array_end_pos(reader, type : String, size : Int | UInt32, count_width : Int32, count : Int | UInt32) : Int32
      payload_size = validate_compound_header(reader, type, size, count_width, count)
      if count > 0 && payload_size < 1
        raise DecodeError.new("#{type} with values is missing constructor")
      end
      reader.pos + payload_size
    end

    private def validate_compound_header(reader, type : String, size : Int | UInt32, count_width : Int32, count : Int | UInt32) : Int32
      if size < count_width
        raise DecodeError.new("#{type} size #{size} smaller than count field")
      end
      if count > MAX_COMPOUND_VALUES
        raise DecodeError.new("#{type} count #{count} exceeds maximum #{MAX_COMPOUND_VALUES}")
      end
      payload_size = size - count_width
      if payload_size > reader.bytesize - reader.pos
        raise DecodeError.new("#{type} size #{size} exceeds remaining frame payload")
      end
      payload_size.to_i
    end

    private def validate_map_count(count : Int | UInt32) : Nil
      raise DecodeError.new("map count #{count} is not even") unless count.even?
    end

    private def decode_array_item(reader, constructor : UInt8) : Value
      case constructor
      when 0xa3
        size = read_byte(reader).to_i
        Value.symbol(reader.read_string(size))
      when 0xb3
        size = read_size32(reader, "symbol32")
        Value.symbol(reader.read_string(size))
      when 0x70
        Value.uint(reader.read_bytes(UInt32, IO::ByteFormat::NetworkEndian))
      when 0x80
        Value.ulong(reader.read_bytes(UInt64, IO::ByteFormat::NetworkEndian))
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

    # Shared with TransferCodec's direct-field writers below, and with
    # client.cr/types.cr's direct-field performative senders -- a single
    # implementation for the "described-list header" shape used throughout
    # AMQP 1.0's wire format, not a generic Value/Array round-trip.
    def write_descriptor(io : IO, code : UInt64) : Nil
      io.write_byte 0x00_u8
      write_ulong(io, code)
    end

    def write_list_header(io : IO, fields_size : Int32, count : Int32) : Nil
      write_compound_header(io, 0xc0_u8, 0xd0_u8, fields_size, count)
    end

    def write_compound_header(io : IO, code8 : UInt8, code32 : UInt8, fields_size : Int32, count : Int32) : Nil
      if fields_size + 1 <= UInt8::MAX && count <= UInt8::MAX
        io.write_byte code8
        io.write_byte((fields_size + 1).to_u8)
        io.write_byte count.to_u8
      else
        io.write_byte code32
        write_u32(io, (fields_size + 4).to_u32)
        write_u32(io, count.to_u32)
      end
    end

    def list_header_size(fields_size) : Int32
      fields_size + 1 <= UInt8::MAX ? 3 : 9
    end

    def uint_size(value) : Int32
      value = value.to_u64
      value.zero? ? 1 : value <= UInt8::MAX ? 2 : 5
    end

    def string_size(value : String) : Int32
      (value.bytesize <= UInt8::MAX ? 2 : 5) + value.bytesize
    end

    def nullable_string_size(value : String?) : Int32
      value ? string_size(value) : 1
    end

    def write_nullable_string(io : IO, value : String?) : Nil
      value ? write_string(io, value) : io.write_byte(0x40_u8)
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
        constructor = values.any? { |v| v.string_value.bytesize > UInt8::MAX } ? 0xb3_u8 : 0xa3_u8
        length_size = constructor == 0xa3_u8 ? 1 : 4
        elements_size = values.sum(0) { |v| length_size + v.string_value.bytesize }
        payload = 2 + elements_size
        if payload <= UInt8::MAX && values.size <= UInt8::MAX
          io.write_byte 0xe0_u8
          io.write_byte payload.to_u8
          io.write_byte values.size.to_u8
        else
          io.write_byte 0xf0_u8
          write_u32(io, (5 + elements_size).to_u32)
          write_u32(io, values.size.to_u32)
        end
        io.write_byte constructor
        values.each do |v|
          sym = v.string_value
          if constructor == 0xa3_u8
            io.write_byte sym.bytesize.to_u8
          else
            write_u32(io, sym.bytesize.to_u32)
          end
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
