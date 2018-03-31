require "io/memory"

module AvalancheMQ
  module AMQP
    module IO
      def write_short_string(str : String)
        raise "String too long" if str.bytesize > 255
        write_byte(str.bytesize.to_u8)
        write_utf8(str.to_slice)
      end

      def write_bool(val : Bool)
        write_byte(val ? 1_u8 : 0_u8)
      end

      def write_int(int)
        write_bytes(int, ::IO::ByteFormat::BigEndian)
      end

      def write_long_string(str : String)
        size = write_bytes(str.bytesize.to_u32, ::IO::ByteFormat::BigEndian)
        write_utf8(str.to_slice)
      end

      def write_table(hash : Hash(String, Field))
        tmp = MemoryIO.new
        hash.each do |key, value|
          tmp.write_short_string(key)
          case value
          when String
            tmp.write_byte 'S'.ord.to_u8
            tmp.write_long_string(value)
          when UInt8
            tmp.write_byte 'b'.ord.to_u8
            tmp.write_byte(value)
          when UInt16
            tmp.write_byte 's'.ord.to_u8
            tmp.write_int(value)
          when Int32
            tmp.write_byte 'I'.ord.to_u8
            tmp.write_int(value)
          when Int64
            tmp.write_byte 'l'.ord.to_u8
            tmp.write_int(value)
          when Time
            tmp.write_byte 'T'.ord.to_u8
            tmp.write_timestamp(value)
          when Hash(String, Field)
            tmp.write_byte 'F'.ord.to_u8
            tmp.write_table(value)
          when Bool
            tmp.write_byte 't'.ord.to_u8
            tmp.write_bool(value)
          when nil
            tmp.write_byte 'V'.ord.to_u8
            nil
          else raise "Unknown type: #{value.class}"
          end
        end
        write_int(tmp.size.to_u32)
        write tmp.to_slice
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
        when 'f' then raise "Cannot parse float32"
        when 'd' then raise "Cannot parse float64"
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
        ms = read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        Time.epoch_ms(ms)
      end

      def write_timestamp(ts)
        write_bytes(ts.epoch.to_i64, ::IO::ByteFormat::BigEndian)
      end

      def read_uint32
        read_bytes(UInt32, ::IO::ByteFormat::BigEndian)
      end

      def read_uint64
        read_bytes(UInt64, ::IO::ByteFormat::BigEndian)
      end

      def read_uint16
        read_bytes(UInt16, ::IO::ByteFormat::BigEndian)
      end

      def read_long_string
        size = read_bytes(UInt32, ::IO::ByteFormat::BigEndian)
        read_string(size)
      end

      def read_uint32
        read_bytes(UInt32, ::IO::ByteFormat::BigEndian)
      end

      def read_int32
        read_bytes(Int32, ::IO::ByteFormat::BigEndian)
      end

      def read_int64
        read_bytes(Int64, ::IO::ByteFormat::BigEndian)
      end
    end

    class MemoryIO < ::IO::Memory
      include AMQP::IO
    end
  end
end
