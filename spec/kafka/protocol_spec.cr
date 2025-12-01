require "./spec_helper"

include KafkaHelpers

describe "Kafka Protocol" do
  describe "ApiVersions" do
    it "responds with supported API versions" do
      with_kafka_server do |_s, kafka_port|
        socket = TCPSocket.new("localhost", kafka_port)
        socket.read_timeout = 5.seconds

        # Build ApiVersions request (v0)
        body = IO::Memory.new
        body.write_bytes(18_i16, IO::ByteFormat::BigEndian) # api_key = ApiVersions
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)  # api_version = 0
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian)  # correlation_id = 1
        body.write_bytes(11_i16, IO::ByteFormat::BigEndian) # client_id length
        body.write("test-client".to_slice)

        # Send size-prefixed message
        socket.write_bytes(body.size.to_i32, IO::ByteFormat::BigEndian)
        socket.write(body.to_slice)
        socket.flush

        # Read response
        _size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        correlation_id = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        error_code = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        api_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)

        correlation_id.should eq 1
        error_code.should eq 0
        api_count.should eq 3

        # Read API versions
        api_keys = [] of Int16
        api_count.times do
          api_key = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
          _min_version = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
          _max_version = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
          api_keys << api_key
        end

        api_keys.should contain(0_i16)  # Produce
        api_keys.should contain(3_i16)  # Metadata
        api_keys.should contain(18_i16) # ApiVersions

        socket.close
      end
    end
  end

  describe "Metadata" do
    it "responds with broker and topic metadata" do
      with_kafka_server do |s, kafka_port|
        # First, create a stream queue
        vhost = s.vhosts["/"]
        args = LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})
        vhost.declare_queue("test-stream", durable: true, auto_delete: false, arguments: args)

        socket = TCPSocket.new("localhost", kafka_port)
        socket.read_timeout = 5.seconds

        # Send ApiVersions first (required by some clients)
        body = IO::Memory.new
        body.write_bytes(18_i16, IO::ByteFormat::BigEndian)
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian)
        body.write_bytes(-1_i16, IO::ByteFormat::BigEndian) # null client_id
        socket.write_bytes(body.size.to_i32, IO::ByteFormat::BigEndian)
        socket.write(body.to_slice)
        socket.flush

        # Skip ApiVersions response
        _size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        socket.skip(_size)

        # Build Metadata request (v0)
        body = IO::Memory.new
        body.write_bytes(3_i16, IO::ByteFormat::BigEndian)  # api_key = Metadata
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)  # api_version = 0
        body.write_bytes(2_i32, IO::ByteFormat::BigEndian)  # correlation_id = 2
        body.write_bytes(-1_i16, IO::ByteFormat::BigEndian) # null client_id
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian)  # topics array length = 1
        topic_name = "test-stream"
        body.write_bytes(topic_name.bytesize.to_i16, IO::ByteFormat::BigEndian)
        body.write(topic_name.to_slice)

        socket.write_bytes(body.size.to_i32, IO::ByteFormat::BigEndian)
        socket.write(body.to_slice)
        socket.flush

        # Read response
        _size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        correlation_id = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)

        correlation_id.should eq 2

        # Read brokers array
        broker_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        broker_count.should eq 1

        # Read first broker
        node_id = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        host_len = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        host = Bytes.new(host_len)
        socket.read_fully(host)
        port = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)

        node_id.should eq 1
        host_str = String.new(host)
        # Accept both IPv4 and IPv6 localhost
        (host_str == "127.0.0.1" || host_str == "::1" || host_str == "localhost").should be_true
        port.should eq kafka_port

        # Read topics array
        topic_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        topic_count.should eq 1

        # Read first topic
        error_code = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        name_len = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        name = Bytes.new(name_len)
        socket.read_fully(name)

        error_code.should eq 0
        String.new(name).should eq "test-stream"

        # Read partitions
        partition_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        partition_count.should eq 1

        socket.close
      end
    end
  end

  describe "Produce" do
    it "stores messages in stream queue" do
      with_kafka_server do |s, kafka_port|
        # First, create a stream queue
        vhost = s.vhosts["/"]
        args = LavinMQ::AMQP::Table.new({"x-queue-type" => "stream"})
        vhost.declare_queue("produce-test", durable: true, auto_delete: false, arguments: args)

        socket = TCPSocket.new("localhost", kafka_port)
        socket.read_timeout = 5.seconds

        # Send ApiVersions first
        body = IO::Memory.new
        body.write_bytes(18_i16, IO::ByteFormat::BigEndian)
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian)
        body.write_bytes(-1_i16, IO::ByteFormat::BigEndian)
        socket.write_bytes(body.size.to_i32, IO::ByteFormat::BigEndian)
        socket.write(body.to_slice)
        socket.flush
        _size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        socket.skip(_size)

        # Build a minimal RecordBatch
        record_batch = build_record_batch("Hello, Kafka!")

        # Build Produce request (v0)
        body = IO::Memory.new
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)    # api_key = Produce
        body.write_bytes(0_i16, IO::ByteFormat::BigEndian)    # api_version = 0
        body.write_bytes(2_i32, IO::ByteFormat::BigEndian)    # correlation_id = 2
        body.write_bytes(-1_i16, IO::ByteFormat::BigEndian)   # null client_id
        body.write_bytes(1_i16, IO::ByteFormat::BigEndian)    # acks = 1 (leader only)
        body.write_bytes(5000_i32, IO::ByteFormat::BigEndian) # timeout_ms

        # topics array
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian) # topics count
        topic_name = "produce-test"
        body.write_bytes(topic_name.bytesize.to_i16, IO::ByteFormat::BigEndian)
        body.write(topic_name.to_slice)

        # partitions array
        body.write_bytes(1_i32, IO::ByteFormat::BigEndian)                    # partitions count
        body.write_bytes(0_i32, IO::ByteFormat::BigEndian)                    # partition index
        body.write_bytes(record_batch.size.to_i32, IO::ByteFormat::BigEndian) # record set size
        body.write(record_batch.to_slice)

        socket.write_bytes(body.size.to_i32, IO::ByteFormat::BigEndian)
        socket.write(body.to_slice)
        socket.flush

        # Read response
        _size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        correlation_id = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)

        correlation_id.should eq 2

        # Read responses array
        topic_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        topic_count.should eq 1

        # Read topic response
        name_len = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        name = Bytes.new(name_len)
        socket.read_fully(name)
        String.new(name).should eq "produce-test"

        # Read partition responses array
        partition_count = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        partition_count.should eq 1

        # Read partition response
        partition_index = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        error_code = socket.read_bytes(Int16, IO::ByteFormat::BigEndian)
        base_offset = socket.read_bytes(Int64, IO::ByteFormat::BigEndian)

        partition_index.should eq 0
        error_code.should eq 0
        base_offset.should be >= 0

        socket.close

        # Verify message was stored
        stream = vhost.queues["produce-test"].as(LavinMQ::AMQP::Stream)
        stream.message_count.should eq 1
      end
    end
  end
end

# Helper to build a minimal RecordBatch
def build_record_batch(payload : String) : Bytes
  io = IO::Memory.new

  # Record (inside the batch)
  record = IO::Memory.new
  record.write_byte(0u8)                 # attributes
  write_varint(record, 0)                # timestamp delta
  write_varint(record, 0)                # offset delta
  write_varint(record, -1)               # key length (-1 = null)
  write_varint(record, payload.bytesize) # value length
  record.write(payload.to_slice)
  write_varint(record, 0) # headers count

  record_bytes = record.to_slice
  record_size = record_bytes.size

  # RecordBatch header
  io.write_bytes(0_i64, IO::ByteFormat::BigEndian) # baseOffset
  # batchLength will be calculated after
  _batch_start = io.pos
  io.write_bytes(0_i32, IO::ByteFormat::BigEndian)  # placeholder for batchLength
  io.write_bytes(-1_i32, IO::ByteFormat::BigEndian) # partitionLeaderEpoch
  io.write_byte(2u8)                                # magic (v2)
  # CRC placeholder - we'll skip CRC for now
  io.write_bytes(0_u32, IO::ByteFormat::BigEndian)               # crc
  io.write_bytes(0_i16, IO::ByteFormat::BigEndian)               # attributes (no compression)
  io.write_bytes(0_i32, IO::ByteFormat::BigEndian)               # lastOffsetDelta
  io.write_bytes(Time.utc.to_unix_ms, IO::ByteFormat::BigEndian) # firstTimestamp
  io.write_bytes(Time.utc.to_unix_ms, IO::ByteFormat::BigEndian) # maxTimestamp
  io.write_bytes(-1_i64, IO::ByteFormat::BigEndian)              # producerId
  io.write_bytes(-1_i16, IO::ByteFormat::BigEndian)              # producerEpoch
  io.write_bytes(-1_i32, IO::ByteFormat::BigEndian)              # baseSequence
  io.write_bytes(1_i32, IO::ByteFormat::BigEndian)               # records count

  # Write the record with its varint length prefix
  write_varint(io, record_size)
  io.write(record_bytes)

  # Go back and fix batchLength
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
