require "./spec_helper"
require "../src/lavinmq/kafka/protocol"

describe LavinMQ::Kafka::Protocol do
  it "writes ApiVersionsResponse correctly" do
    io = IO::Memory.new
    protocol = LavinMQ::Kafka::Protocol.new(io)

    api_versions = [
      LavinMQ::Kafka::ApiVersion.new(0_i16, 0_i16, 3_i16),
      LavinMQ::Kafka::ApiVersion.new(1_i16, 0_i16, 2_i16),
    ]
    response = LavinMQ::Kafka::ApiVersionsResponse.new(
      api_version: 0_i16,
      correlation_id: 123_i32,
      error_code: 0_i16,
      api_versions: api_versions
    )

    protocol.write_response(response)

    io.rewind
    size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    correlation_id = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    error_code = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
    array_length = io.read_bytes(Int32, IO::ByteFormat::BigEndian)

    size.should eq(response.bytesize)
    correlation_id.should eq(123)
    error_code.should eq(0)
    array_length.should eq(2)
  end

  it "writes MetadataResponse correctly" do
    io = IO::Memory.new
    protocol = LavinMQ::Kafka::Protocol.new(io)

    api_version = 1_i16
    brokers = [
      LavinMQ::Kafka::BrokerMetadata.new(1_i32, "localhost", 9092_i32, api_version, nil),
    ]
    topics = [
      LavinMQ::Kafka::TopicMetadata.new(
        error_code: 0_i16,
        name: "test-topic",
        internal: false,
        partitions: [
          LavinMQ::Kafka::PartitionMetadata.new(
            error_code: 0_i16,
            partition_index: 0_i32,
            leader_id: 1_i32,
            replica_nodes: [1_i32],
            isr_nodes: [1_i32]
          ),
        ],
        api_version: api_version
      ),
    ]

    response = LavinMQ::Kafka::MetadataResponse.new(
      api_version: api_version,
      correlation_id: 456_i32,
      brokers: brokers,
      cluster_id: "test-cluster",
      controller_id: 1_i32,
      topics: topics
    )

    protocol.write_response(response)

    io.rewind
    size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    correlation_id = io.read_bytes(Int32, IO::ByteFormat::BigEndian)

    size.should eq(response.bytesize)
    correlation_id.should eq(456)
  end

  it "writes ProduceResponse correctly" do
    io = IO::Memory.new
    protocol = LavinMQ::Kafka::Protocol.new(io)

    api_version = 1_i16
    partition_responses = [
      LavinMQ::Kafka::PartitionProduceResponse.new(
        index: 0_i32,
        error_code: 0_i16,
        base_offset: 100_i64,
        api_version: api_version,
        log_append_time_ms: -1_i64
      ),
    ]
    topic_responses = [
      LavinMQ::Kafka::TopicProduceResponse.new(
        name: "test-topic",
        partition_responses: partition_responses,
        api_version: api_version
      ),
    ]

    response = LavinMQ::Kafka::ProduceResponse.new(
      api_version: api_version,
      correlation_id: 789_i32,
      responses: topic_responses,
      throttle_time_ms: 0_i32
    )

    protocol.write_response(response)

    io.rewind
    size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    correlation_id = io.read_bytes(Int32, IO::ByteFormat::BigEndian)

    size.should eq(response.bytesize)
    correlation_id.should eq(789)
  end

  it "calculates bytesize correctly for ApiVersionsResponse with throttle" do
    api_versions = [
      LavinMQ::Kafka::ApiVersion.new(0_i16, 0_i16, 3_i16),
    ]

    response_v0 = LavinMQ::Kafka::ApiVersionsResponse.new(
      api_version: 0_i16,
      correlation_id: 1_i32,
      error_code: 0_i16,
      api_versions: api_versions,
      throttle_time_ms: 100_i32
    )

    response_v1 = LavinMQ::Kafka::ApiVersionsResponse.new(
      api_version: 1_i16,
      correlation_id: 1_i32,
      error_code: 0_i16,
      api_versions: api_versions,
      throttle_time_ms: 100_i32
    )

    # v1 should be 4 bytes larger (throttle_time_ms)
    (response_v1.bytesize - response_v0.bytesize).should eq(4)
  end
end
