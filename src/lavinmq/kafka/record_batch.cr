require "compress/gzip"
require "lz4"

module LavinMQ
  module Kafka
    # Parses Kafka RecordBatch format (v2, magic byte 2)
    # https://kafka.apache.org/documentation/#recordbatch
    class RecordBatch
      # Reusable hash for header parsing to avoid per-record allocations
      # Note: This is shared across all parsing calls, so it's not thread-safe
      # for concurrent parsing, but our architecture processes one request at a time
      @@headers_reuse_buffer = Hash(String, Bytes).new

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

      def self.parse(io : ::IO, length : Int32) : Array(RecordBatch)
        batches = [] of RecordBatch
        bytes_remaining = length

        begin
          loop do
            break if bytes_remaining <= 0
            batch = parse_one(io)
            if batch
              batches << batch
              # Each batch consumes: base_offset(8) + batch_length(4) + batch_length bytes
              bytes_remaining -= (12 + batch.batch_length)
            else
              break
            end
          end
        rescue ::IO::EOFError
          # Done reading all batches
        end

        batches
      end

      # Streaming version - yields each record without accumulating
      # Returns total record count processed
      def self.parse_each(io : ::IO, length : Int32, base_offset_start : Int64, &block : Int64, Record -> Nil) : Int32
        bytes_remaining = length
        total_records = 0

        begin
          loop do
            break if bytes_remaining <= 0
            count, batch_size = parse_one_streaming(io, base_offset_start + total_records, &block)
            if count
              total_records += count
              bytes_remaining -= batch_size
            else
              break
            end
          end
        rescue ::IO::EOFError
          # Done reading all batches
        end

        total_records
      end

      # Streaming version of parse_one that yields records
      # Returns tuple of {record_count, bytes_consumed} or {nil, 0} if no batch
      private def self.parse_one_streaming(io : ::IO, base_offset_start : Int64, &block : Int64, Record -> Nil) : {Int32?, Int32}
        base_offset = io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
        batch_length = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)

        # Total bytes consumed: base_offset(8) + batch_length(4) + batch_length
        batch_size = 12 + batch_length

        return {nil, batch_size} if batch_length <= 0

        partition_leader_epoch = io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)
        magic = io.read_bytes(Int8, ::IO::ByteFormat::BigEndian)

        if magic != 2
          # Skip legacy message formats
          io.skip(batch_length - 5)
          return {nil, batch_size}
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
        records_data_size = batch_length - 49

        if compression == Compression::None
          parse_records_streaming(io, record_count, base_offset, first_timestamp, base_offset_start, &block)
        else
          # For compressed batches, we still need to decompress first
          compressed_data = Bytes.new(records_data_size)
          io.read_fully(compressed_data)
          decompressed_io = decompress(compressed_data, compression)
          parse_records_streaming(decompressed_io, record_count, base_offset, first_timestamp, base_offset_start, &block)
        end

        {record_count, batch_size}
      end

      # Streaming version of parse_records that yields each record
      private def self.parse_records_streaming(io : ::IO, count : Int32, base_offset : Int64, first_timestamp : Int64, base_offset_start : Int64, &block : Int64, Record -> Nil) : Nil
        count.times do |i|
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
                  else
                    Bytes.empty
                  end

          headers_count = read_varint(io)

          # Clear and reuse the headers buffer to avoid per-record allocation
          @@headers_reuse_buffer.clear

          headers_count.times do
            header_key_length = read_varint(io)
            header_key = if header_key_length > 0
                           io.read_string(header_key_length)
                         else
                           ""
                         end

            header_value_length = read_varint(io)
            header_value = if header_value_length > 0
                             bytes = Bytes.new(header_value_length)
                             io.read_fully(bytes)
                             bytes
                           else
                             Bytes.empty
                           end

            @@headers_reuse_buffer[header_key] = header_value
          end

          timestamp = first_timestamp + timestamp_delta
          offset = base_offset + offset_delta
          record = Record.new(offset, timestamp, key, value, @@headers_reuse_buffer)

          # Yield record with its stream offset
          # IMPORTANT: Caller must process record.headers immediately before next iteration
          # as the headers hash is reused across records
          yield base_offset_start + i, record
        end
      end

      private def self.parse_one(io : ::IO) : RecordBatch?
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
        Array(Record).new(count) do
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
                  else
                    Bytes.empty
                  end

          headers_count = read_varint(io)
          headers = Hash(String, Bytes).new(initial_capacity: headers_count)

          headers_count.times do
            header_key_length = read_varint(io)
            header_key = if header_key_length > 0
                           io.read_string(header_key_length)
                         else
                           ""
                         end

            header_value_length = read_varint(io)
            header_value = if header_value_length > 0
                             bytes = Bytes.new(header_value_length)
                             io.read_fully(bytes)
                             bytes
                           else
                             Bytes.empty
                           end

            headers[header_key] = header_value
          end

          Record.new(
            offset: base_offset + offset_delta,
            timestamp: first_timestamp + timestamp_delta,
            key: key,
            value: value,
            headers: headers
          )
        end
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
