require "io/memory"

module AMQPServer
  module AMQP
    class IO < IO::Memory
      def write_short_string(str : String)
        raise "String too long" if str.bytesize > 255
        write_byte(str.bytesize.to_u8)
        write_utf8(str.to_slice)
      end

      def write_bool(val : Bool)
        write_byte(val ? 1_u8 : 0_u8)
      end

      def write_int(int)
        write_bytes(int, IO::ByteFormat::BigEndian)
      end

      def write_long_string(str : String)
        size = write_bytes(str.bytesize.to_u32, IO::ByteFormat::BigEndian)
        write_utf8(str.to_slice)
      end

      def write_table(hash : Hash(String, Field))
        tmp = AMQP::IO.new
        hash.each do |key, value|
          tmp.write_short_string(key)
          case value
          when String
            tmp.write_byte 'S'.ord.to_u8
            tmp.write_long_string(value)
          when Int
            tmp.write_byte 'I'.ord.to_u8
            tmp.write_int(value)
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
        write_int(tmp.size)
        write tmp.to_slice
      end

      def read_byte : UInt8
        b = super()
        raise EOFError.new("Byte was nil") if b.nil?
        b
      end

      def read_table
        sz = read_uint32
        start_pos = pos
        hash = Hash(String, Field).new
        while pos < start_pos + sz
          key = read_short_string
          type = read_byte
          val = case type
                when 'S' then read_long_string
                when 'I' then read_int
                when 'l' then read_uint64
                when 'F' then read_table
                when 't' then read_bool
                when 'V' then nil
                else raise "Unknown type: #{type}"
                end
          hash[key] = val
        end
        hash
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

      def read_uint32
        read_bytes(UInt32, IO::ByteFormat::BigEndian)
      end

      def read_uint64
        read_bytes(UInt64, IO::ByteFormat::BigEndian)
      end

      def read_uint16
        read_bytes(UInt16, IO::ByteFormat::BigEndian)
      end

      def read_long_string
        size = read_bytes(UInt32, IO::ByteFormat::BigEndian)
        read_string(size)
      end

      def read_int
        read_bytes(UInt32, IO::ByteFormat::BigEndian)
      end
    end
  end
end
