require "../spec_helper"
require "../../src/lavinmq/kafka/record_batch"

describe LavinMQ::Kafka::RecordBatch do
  describe ".parse" do
    it "parses uncompressed record batch" do
      batch_data = build_record_batch("test message", compression: LavinMQ::Kafka::RecordBatch::Compression::None)
      io = IO::Memory.new(batch_data)
      batches = LavinMQ::Kafka::RecordBatch.parse(io, batch_data.size)

      batches.size.should eq 1
      batch = batches.first
      batch.records.size.should eq 1
      batch.compression.should eq LavinMQ::Kafka::RecordBatch::Compression::None
      String.new(batch.records.first.value).should eq "test message"
    end

    it "parses gzip compressed record batch" do
      batch_data = build_record_batch("compressed with gzip", compression: LavinMQ::Kafka::RecordBatch::Compression::Gzip)
      io = IO::Memory.new(batch_data)
      batches = LavinMQ::Kafka::RecordBatch.parse(io, batch_data.size)

      batches.size.should eq 1
      batch = batches.first
      batch.records.size.should eq 1
      batch.compression.should eq LavinMQ::Kafka::RecordBatch::Compression::Gzip
      String.new(batch.records.first.value).should eq "compressed with gzip"
    end

    it "parses lz4 compressed record batch" do
      batch_data = build_record_batch("compressed with lz4", compression: LavinMQ::Kafka::RecordBatch::Compression::Lz4)
      io = IO::Memory.new(batch_data)
      batches = LavinMQ::Kafka::RecordBatch.parse(io, batch_data.size)

      batches.size.should eq 1
      batch = batches.first
      batch.records.size.should eq 1
      batch.compression.should eq LavinMQ::Kafka::RecordBatch::Compression::Lz4
      String.new(batch.records.first.value).should eq "compressed with lz4"
    end

    it "parses multiple records in compressed batch" do
      batch_data = build_record_batch(
        ["message 1", "message 2", "message 3"],
        compression: LavinMQ::Kafka::RecordBatch::Compression::Gzip
      )
      io = IO::Memory.new(batch_data)
      batches = LavinMQ::Kafka::RecordBatch.parse(io, batch_data.size)

      batches.size.should eq 1
      batch = batches.first
      batch.records.size.should eq 3
      String.new(batch.records[0].value).should eq "message 1"
      String.new(batch.records[1].value).should eq "message 2"
      String.new(batch.records[2].value).should eq "message 3"
    end
  end
end

# Helper to build a RecordBatch with optional compression
def build_record_batch(payload : String | Array(String), compression : LavinMQ::Kafka::RecordBatch::Compression = LavinMQ::Kafka::RecordBatch::Compression::None) : Bytes
  payloads = payload.is_a?(String) ? [payload] : payload

  # Build records data (uncompressed)
  records_io = IO::Memory.new
  payloads.each_with_index do |msg, idx|
    record = IO::Memory.new
    record.write_byte(0u8)             # attributes
    write_varint(record, 0)            # timestamp delta
    write_varint(record, idx)          # offset delta
    write_varint(record, -1)           # key length (-1 = null)
    write_varint(record, msg.bytesize) # value length
    record.write(msg.to_slice)
    write_varint(record, 0) # headers count

    record_bytes = record.to_slice
    write_varint(records_io, record_bytes.size)
    records_io.write(record_bytes)
  end

  records_data = records_io.to_slice

  # Compress if needed
  final_records_data = case compression
                       when LavinMQ::Kafka::RecordBatch::Compression::None
                         records_data
                       when LavinMQ::Kafka::RecordBatch::Compression::Gzip
                         compressed = IO::Memory.new
                         Compress::Gzip::Writer.open(compressed) do |gzip|
                           gzip.write(records_data)
                         end
                         compressed.to_slice
                       when LavinMQ::Kafka::RecordBatch::Compression::Lz4
                         compressed = IO::Memory.new
                         Compress::LZ4::Writer.open(compressed) do |lz4|
                           lz4.write(records_data)
                         end
                         compressed.to_slice
                       else
                         raise "Unsupported compression type"
                       end

  # Build RecordBatch header
  io = IO::Memory.new
  io.write_bytes(0_i64, IO::ByteFormat::BigEndian)                      # baseOffset
  io.write_bytes(0_i32, IO::ByteFormat::BigEndian)                      # placeholder for batchLength
  io.write_bytes(-1_i32, IO::ByteFormat::BigEndian)                     # partitionLeaderEpoch
  io.write_byte(2u8)                                                    # magic (v2)
  io.write_bytes(0_u32, IO::ByteFormat::BigEndian)                      # crc (placeholder)
  io.write_bytes(compression.value.to_i16, IO::ByteFormat::BigEndian)   # attributes (compression type in lower 3 bits)
  io.write_bytes((payloads.size - 1).to_i32, IO::ByteFormat::BigEndian) # lastOffsetDelta
  io.write_bytes(Time.utc.to_unix_ms, IO::ByteFormat::BigEndian)        # firstTimestamp
  io.write_bytes(Time.utc.to_unix_ms, IO::ByteFormat::BigEndian)        # maxTimestamp
  io.write_bytes(-1_i64, IO::ByteFormat::BigEndian)                     # producerId
  io.write_bytes(-1_i16, IO::ByteFormat::BigEndian)                     # producerEpoch
  io.write_bytes(-1_i32, IO::ByteFormat::BigEndian)                     # baseSequence
  io.write_bytes(payloads.size.to_i32, IO::ByteFormat::BigEndian)       # records count

  # Write records data (compressed or not)
  io.write(final_records_data)

  # Fix batchLength
  result = io.to_slice
  batch_length = result.size - 12 # Subtract baseOffset (8) + batchLength field (4)
  IO::ByteFormat::BigEndian.encode(batch_length.to_i32, result[8, 4])

  result
end

def write_varint(io : IO, value : Int32)
  # ZigZag encode
  encoded = (value << 1) ^ (value >> 31)
  while encoded > 0x7f
    io.write_byte(((encoded & 0x7f) | 0x80).to_u8)
    encoded = encoded >> 7
  end
  io.write_byte(encoded.to_u8)
end
