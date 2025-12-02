require "compress/gzip"
require "lz4"

module LavinMQ
  module Kafka
    # Parses Kafka RecordBatch format (v2, magic byte 2)
    # https://kafka.apache.org/documentation/#recordbatch
    class RecordBatch
      getter base_offset : Int64
      getter batch_length : Int32
      getter partition_leader_epoch : Int32
      getter magic : Int8
      getter crc : UInt32
      getter attributes : Int16
      getter last_offset_delta : Int32
      getter first_timestamp : Int64
      getter max_timestamp : Int64
      getter producer_id : Int64
      getter producer_epoch : Int16
      getter base_sequence : Int32
      getter records : Array(Record)

      def initialize(@base_offset, @batch_length, @partition_leader_epoch, @magic, @crc,
                     @attributes, @last_offset_delta, @first_timestamp, @max_timestamp,
                     @producer_id, @producer_epoch, @base_sequence, @records)
      end

      # Compression type from attributes
      def compression : Compression
        Compression.new((@attributes & 0x07).to_i8)
      end

      def timestamp_type : TimestampType
        if (@attributes & 0x08) != 0
          TimestampType::LogAppendTime
        else
          TimestampType::CreateTime
        end
      end

      def transactional? : Bool
        (@attributes & 0x10) != 0
      end

      def control? : Bool
        (@attributes & 0x20) != 0
      end

      enum Compression
        None   = 0
        Gzip   = 1
        Snappy = 2
        Lz4    = 3
        Zstd   = 4
      end

      enum TimestampType
        CreateTime
        LogAppendTime
      end

      def self.parse(bytes : Bytes) : Array(RecordBatch)
        batches = [] of RecordBatch
        io = ::IO::Memory.new(bytes)

        while io.pos < bytes.size
          batch = parse_one(io)
          batches << batch if batch
        end

        batches
      end

      private def self.parse_one(io : ::IO) : RecordBatch?
        return nil if io.pos >= io.size

        base_offset = io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        batch_length = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)

        return nil if batch_length <= 0

        partition_leader_epoch = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)
        magic = io.read_bytes(Int8, ::IO::ByteFormat::BigEndian)

        if magic != 2
          # Skip legacy message formats, just return empty batch
          io.skip(batch_length - 5) # Already read 5 bytes (partition_leader_epoch + magic)
          return nil
        end

        crc = io.read_bytes(UInt32, ::IO::ByteFormat::BigEndian)
        attributes = io.read_bytes(Int16, ::IO::ByteFormat::BigEndian)
        last_offset_delta = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)
        first_timestamp = io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        max_timestamp = io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        producer_id = io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        producer_epoch = io.read_bytes(Int16, ::IO::ByteFormat::BigEndian)
        base_sequence = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)
        record_count = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)

        compression = Compression.new((attributes & 0x07).to_i8)

        # Calculate remaining bytes in the batch for record data
        # batch_length includes everything after the batch_length field itself
        # We've read: partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
        #             last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) +
        #             producer_id(8) + producer_epoch(2) + base_sequence(4) + record_count(4) = 49 bytes
        records_data_size = batch_length - 49

        records = if compression == Compression::None
                    parse_records(io, record_count, base_offset, first_timestamp)
                  else
                    # Read compressed data
                    compressed_data = Bytes.new(records_data_size)
                    io.read_fully(compressed_data)

                    # Decompress based on compression type
                    decompressed_io = decompress(compressed_data, compression)
                    parse_records(decompressed_io, record_count, base_offset, first_timestamp)
                  end

        RecordBatch.new(
          base_offset, batch_length, partition_leader_epoch, magic, crc,
          attributes, last_offset_delta, first_timestamp, max_timestamp,
          producer_id, producer_epoch, base_sequence, records
        )
      end

      private def self.decompress(data : Bytes, compression : Compression) : ::IO::Memory
        case compression
        when Compression::Gzip
          decompressed = ::IO::Memory.new
          Compress::Gzip::Reader.open(::IO::Memory.new(data)) do |gzip|
            ::IO.copy(gzip, decompressed)
          end
          decompressed.rewind
          decompressed
        when Compression::Lz4
          # Kafka uses LZ4 block format (not frame format)
          # The LZ4 shard supports the frame format, so we use that
          decompressed = ::IO::Memory.new
          Compress::LZ4::Reader.open(::IO::Memory.new(data)) do |lz4|
            ::IO.copy(lz4, decompressed)
          end
          decompressed.rewind
          decompressed
        when Compression::Snappy, Compression::Zstd
          # Snappy and Zstd not yet supported
          raise Error.new("Compression type #{compression} not yet supported")
        else
          raise Error.new("Unknown compression type: #{compression}")
        end
      end

      private def self.parse_records(io : ::IO, count : Int32, base_offset : Int64, first_timestamp : Int64) : Array(Record)
        records = Array(Record).new(count)

        count.times do
          _length = read_varint(io)
          _attributes = io.read_bytes(Int8, ::IO::ByteFormat::BigEndian)
          timestamp_delta = read_varlong(io)
          offset_delta = read_varint(io)

          key_length = read_varint(io)
          key = if key_length > 0
                  bytes = Bytes.new(key_length)
                  io.read_fully(bytes)
                  bytes
                elsif key_length == 0
                  Bytes.empty
                else
                  nil
                end

          value_length = read_varint(io)
          value = if value_length > 0
                    bytes = Bytes.new(value_length)
                    io.read_fully(bytes)
                    bytes
                  elsif value_length == 0
                    Bytes.empty
                  else
                    Bytes.empty
                  end

          headers_count = read_varint(io)
          headers = Hash(String, Bytes).new

          headers_count.times do
            header_key_length = read_varint(io)
            header_key = if header_key_length > 0
                           bytes = Bytes.new(header_key_length)
                           io.read_fully(bytes)
                           String.new(bytes)
                         else
                           ""
                         end

            header_value_length = read_varint(io)
            header_value = if header_value_length > 0
                             bytes = Bytes.new(header_value_length)
                             io.read_fully(bytes)
                             bytes
                           elsif header_value_length == 0
                             Bytes.empty
                           else
                             Bytes.empty
                           end

            headers[header_key] = header_value
          end

          records << Record.new(
            offset: base_offset + offset_delta,
            timestamp: first_timestamp + timestamp_delta,
            key: key,
            value: value,
            headers: headers
          )
        end

        records
      end

      # Read a varint (zigzag encoded)
      private def self.read_varint(io : ::IO) : Int32
        result = 0
        shift = 0

        loop do
          byte = io.read_byte
          raise ::IO::EOFError.new if byte.nil?

          result |= ((byte & 0x7f).to_i32) << shift
          break if (byte & 0x80) == 0
          shift += 7
        end

        # Zigzag decode
        (result >> 1) ^ (-(result & 1))
      end

      # Read a varlong (zigzag encoded)
      private def self.read_varlong(io : ::IO) : Int64
        result = 0_i64
        shift = 0

        loop do
          byte = io.read_byte
          raise ::IO::EOFError.new if byte.nil?

          result |= ((byte & 0x7f).to_i64) << shift
          break if (byte & 0x80) == 0
          shift += 7
        end

        # Zigzag decode
        (result >> 1) ^ (-(result & 1))
      end
    end

    struct Record
      getter offset : Int64
      getter timestamp : Int64
      getter key : Bytes?
      getter value : Bytes
      getter headers : Hash(String, Bytes)

      def initialize(@offset, @timestamp, @key, @value, @headers)
      end
    end
  end
end
