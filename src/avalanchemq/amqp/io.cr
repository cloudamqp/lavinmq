require "io/memory"

module AvalancheMQ
  module AMQP
    module IO
      def write_int(val)
        write_bytes(val, ::IO::ByteFormat::NetworkEndian)
      end

      def write_short_string(str : String)
        raise "String too long" if str.bytesize > 255
        write_byte(str.bytesize.to_u8)
        write(str.to_slice)
      end

      def write_bool(val : Bool)
        write_byte(val ? 1_u8 : 0_u8)
      end

      def write_long_string(str : String)
        write_bytes(str.bytesize.to_u32, ::IO::ByteFormat::NetworkEndian)
        write(str.to_slice)
      end

      def write_table(hash : Hash(String, Field))
        write_bytes Table.new(hash), ::IO::ByteFormat::NetworkEndian
      end

      def read_byte : UInt8
        b = super()
        raise ::IO::EOFError.new("Byte was nil") if b.nil?
        b
      end

      def read_table
        Table.from_io(self, ::IO::ByteFormat::NetworkEndian)
      end

      def read_short_string
        size = read_byte
        read_string(size.to_i32)
      end

      def read_bool
        int = read_byte
        raise "Unknown boolen value: #{int}" if int.nil? || int > 1
        int == 1
      end

      def read_timestamp
        ms = read_bytes(UInt64, ::IO::ByteFormat::NetworkEndian)
        Time.epoch(ms)
      end

      def write_timestamp(ts)
        write_bytes(ts.epoch.to_u64, ::IO::ByteFormat::NetworkEndian)
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
