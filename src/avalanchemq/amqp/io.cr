require "io/memory"

module AvalancheMQ
  module AMQP
    module IO
      def write_bytes(type)
        super(type, ::IO::ByteFormat::NetworkEndian)
      end

      def write_int(val)
        write_bytes(val)
      end

      def write_short_string(str : String)
        raise "String too long" if str.bytesize > 255
        write_byte(str.bytesize.to_u8)
        write_utf8(str.to_slice)
      end

      def write_bool(val : Bool)
        write_byte(val ? 1_u8 : 0_u8)
      end

      def write_long_string(str : String)
        size = write_bytes(str.bytesize.to_u32)
        write_utf8(str.to_slice)
      end

      def write_table(hash : Hash(String, Field))
        tmp = MemoryIO.new
        hash.each do |key, value|
          tmp.write_short_string(key)
          tmp.write_field(value)
        end
        write_bytes(tmp.size.to_u32)
        write tmp.to_slice
      end

      def write_field(value)
        case value
        when String
          write_byte 'S'.ord.to_u8
          write_long_string(value)
        when UInt8
          write_byte 'b'.ord.to_u8
          write_byte(value)
        when UInt16
          write_byte 's'.ord.to_u8
          write_bytes(value)
        when Int32
          write_byte 'I'.ord.to_u8
          write_bytes(value)
        when Int64
          write_byte 'l'.ord.to_u8
          write_bytes(value)
        when Time
          write_byte 'T'.ord.to_u8
          write_timestamp(value)
        when Hash(String, Field)
          write_byte 'F'.ord.to_u8
          write_table(value)
        when Bool
          write_byte 't'.ord.to_u8
          write_bool(value)
        when Array
          write_byte 'A'.ord.to_u8
          write_array(value)
        when Float32
          write_byte 'f'.ord.to_u8
          write_bytes(value)
        when Float64
          write_byte 'd'.ord.to_u8
          write_bytes(value)
        when nil
          write_byte 'V'.ord.to_u8
          nil
        else raise "Unknown type: #{value.class}"
        end
      end

      def read_byte : UInt8
        b = super()
        raise ::IO::EOFError.new("Byte was nil") if b.nil?
        b
      end

      def read_table
        size = read_uint32
        end_pos = pos + size
        hash = Hash(String, Field).new
        while pos < end_pos
          key = read_short_string
          val = read_field
          hash[key] = val
        end
        hash
      end

      def read_field : Field
        type = read_byte
        case type
        when 'S' then read_long_string
        when 's' then read_uint16
        when 'I' then read_int32
        when 'l' then read_int64
        when 'F' then read_table
        when 't' then read_bool
        when 'T' then read_timestamp
        when 'V' then nil
        when 'b' then read_byte
        when 'A' then read_array
        when 'f' then read_f32
        when 'd' then read_f64
        when 'D' then raise "Cannot parse decimal"
        when 'x' then raise "Cannot parse byte array"
        else raise "Unknown type: #{type}"
        end
      end

      def read_array
        size = read_uint32
        end_pos = pos + size
        a = Array(Field).new
        while pos < end_pos
          a << read_field
        end
        a
      end

      def write_array(array)
        tmp = MemoryIO.new
        array.each do |value|
          tmp.write_field(value)
        end
        write_bytes(tmp.size.to_u32)
        write tmp.to_slice
      end

      def read_short_string
        size = read_byte.as(Int)
        read_string(size)
      end

      def read_bool
        int = read_byte
        raise "Unknown boolen value: #{int}" if int.nil? || int > 1
        int == 1
      end

      def read_timestamp
        ms = read_bytes(Int64, ::IO::ByteFormat::NetworkEndian)
        Time.epoch_ms(ms)
      end

      def write_timestamp(ts)
        write_bytes(ts.epoch.to_i64, ::IO::ByteFormat::NetworkEndian)
      end

      def read_uint32
        read_bytes(UInt32, ::IO::ByteFormat::NetworkEndian)
      end

      def read_uint64
        read_bytes(UInt64, ::IO::ByteFormat::NetworkEndian)
      end

      def read_uint16
        read_bytes(UInt16, ::IO::ByteFormat::NetworkEndian)
      end

      def read_long_string
        size = read_bytes(UInt32, ::IO::ByteFormat::NetworkEndian)
        read_string(size)
      end

      def read_uint32
        read_bytes(UInt32, ::IO::ByteFormat::NetworkEndian)
      end

      def read_int32
        read_bytes(Int32, ::IO::ByteFormat::NetworkEndian)
      end

      def read_int64
        read_bytes(Int64, ::IO::ByteFormat::NetworkEndian)
      end

      def read_f32
        read_bytes(Float32, ::IO::ByteFormat::NetworkEndian)
      end

      def read_f64
        read_bytes(Float64, ::IO::ByteFormat::NetworkEndian)
      end
    end

    class MemoryIO < ::IO::Memory
      include AMQP::IO
    end
  end
end
