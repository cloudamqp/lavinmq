require "./consts"

module LavinMQ
  module Kafka
    class Protocol
      class Error < ::IO::Error
      end

      class MaxRequestSizeError < Error
      end
    end

    # Kafka wire protocol IO wrapper
    # All values are big-endian
    class Protocol
      getter io : ::IO
      # Maximum request size (1 MiB)
      MAX_REQUEST_SIZE = 1_048_576_i32

      @bytes_remaining : UInt32 = 0_u32

      def initialize(@io : ::IO)
      end

      # Read a complete request from the wire
      def read_request : Request
        # Read size directly (not part of the request body)
        size = @io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)

        # Validate request size
        if size < 0
          raise Error.new("Invalid request size: #{size}")
        end
        if size > MAX_REQUEST_SIZE
          raise MaxRequestSizeError.new("Request size #{size} exceeds maximum #{MAX_REQUEST_SIZE}")
        end

        # Track remaining bytes for this request
        @bytes_remaining = size.to_u32

        api_key = read_int16
        api_version = read_int16
        correlation_id = read_int32
        client_id = read_nullable_string

        case api_key
        when ApiKey::ApiVersions
          ApiVersionsRequest.new(api_version, correlation_id, client_id)
        when ApiKey::Metadata
          topics = if api_version >= 1
                     read_nullable_array { read_string }
                   else
                     read_array { read_string }
                   end
          MetadataRequest.new(api_version, correlation_id, client_id, topics)
        when ApiKey::Produce
          acks = read_int16
          timeout_ms = read_int32
          topic_data = read_array do
            topic_name = read_string
            partitions = read_array do
              partition = read_int32
              record_set = read_bytes
              PartitionData.new(partition, record_set)
            end
            TopicData.new(topic_name, partitions)
          end
          ProduceRequest.new(api_version, correlation_id, client_id, acks, timeout_ms, topic_data)
        else
          UnknownRequest.new(api_key, api_version, correlation_id, client_id)
        end
      rescue ex : OverflowError
        raise MaxRequestSizeError.new("Request exceeded size limits")
      end

      # Write a response to the wire
      def write_response(response : Response)
        buffer = ::IO::Memory.new
        write_response_body(buffer, response)
        @io.write_bytes(buffer.size.to_i32, ::IO::ByteFormat::BigEndian)
        @io.write(buffer.to_slice)
      end

      private def write_response_body(io : ::IO, response : Response)
        io.write_bytes(response.correlation_id, ::IO::ByteFormat::BigEndian)

        case response
        when ApiVersionsResponse
          write_api_versions_response(io, response)
        when MetadataResponse
          write_metadata_response(io, response)
        when ProduceResponse
          write_produce_response(io, response)
        end
      end

      private def write_api_versions_response(io : ::IO, response : ApiVersionsResponse)
        io.write_bytes(response.error_code, ::IO::ByteFormat::BigEndian)
        write_array(io, response.api_versions) do |api|
          io.write_bytes(api.api_key, ::IO::ByteFormat::BigEndian)
          io.write_bytes(api.min_version, ::IO::ByteFormat::BigEndian)
          io.write_bytes(api.max_version, ::IO::ByteFormat::BigEndian)
        end
        if response.api_version >= 1
          io.write_bytes(response.throttle_time_ms, ::IO::ByteFormat::BigEndian)
        end
      end

      private def write_metadata_response(io : ::IO, response : MetadataResponse)
        # Brokers array
        write_array(io, response.brokers) do |broker|
          io.write_bytes(broker.node_id, ::IO::ByteFormat::BigEndian)
          write_string(io, broker.host)
          io.write_bytes(broker.port, ::IO::ByteFormat::BigEndian)
          if response.api_version >= 1
            write_nullable_string(io, broker.rack)
          end
        end

        if response.api_version >= 1
          write_nullable_string(io, response.cluster_id)
        end

        if response.api_version >= 1
          io.write_bytes(response.controller_id, ::IO::ByteFormat::BigEndian)
        end

        # Topics array
        write_array(io, response.topics) do |topic|
          io.write_bytes(topic.error_code, ::IO::ByteFormat::BigEndian)
          write_string(io, topic.name)
          if response.api_version >= 1
            io.write_byte(topic.internal? ? 1u8 : 0u8)
          end
          write_array(io, topic.partitions) do |partition|
            io.write_bytes(partition.error_code, ::IO::ByteFormat::BigEndian)
            io.write_bytes(partition.partition_index, ::IO::ByteFormat::BigEndian)
            io.write_bytes(partition.leader_id, ::IO::ByteFormat::BigEndian)
            write_array(io, partition.replica_nodes) do |node|
              io.write_bytes(node, ::IO::ByteFormat::BigEndian)
            end
            write_array(io, partition.isr_nodes) do |node|
              io.write_bytes(node, ::IO::ByteFormat::BigEndian)
            end
          end
        end
      end

      private def write_produce_response(io : ::IO, response : ProduceResponse)
        write_array(io, response.responses) do |topic_response|
          write_string(io, topic_response.name)
          write_array(io, topic_response.partition_responses) do |partition_response|
            io.write_bytes(partition_response.index, ::IO::ByteFormat::BigEndian)
            io.write_bytes(partition_response.error_code, ::IO::ByteFormat::BigEndian)
            io.write_bytes(partition_response.base_offset, ::IO::ByteFormat::BigEndian)
            if response.api_version >= 2
              io.write_bytes(partition_response.log_append_time_ms, ::IO::ByteFormat::BigEndian)
            end
          end
        end
        if response.api_version >= 1
          io.write_bytes(response.throttle_time_ms, ::IO::ByteFormat::BigEndian)
        end
      end

      def flush
        @io.flush
      end

      # Primitive readers
      private def read_int8 : Int8
        @bytes_remaining -= 1_u32
        @io.read_bytes(Int8, ::IO::ByteFormat::BigEndian)
      end

      private def read_int16 : Int16
        @bytes_remaining -= 2_u32
        @io.read_bytes(Int16, ::IO::ByteFormat::BigEndian)
      end

      private def read_int32 : Int32
        @bytes_remaining -= 4_u32
        @io.read_bytes(Int32, ::IO::ByteFormat::BigEndian)
      end

      private def read_int64 : Int64
        @bytes_remaining -= 8_u32
        @io.read_bytes(Int64, ::IO::ByteFormat::BigEndian)
      end

      private def read_string : String
        length = read_int16
        raise Error.new("Invalid string length: #{length}") if length < 0
        @bytes_remaining -= length
        @io.read_string(length)
      end

      private def read_nullable_string : String?
        length = read_int16
        return nil if length < 0
        @bytes_remaining -= length
        @io.read_string(length)
      end

      private def read_bytes : Bytes
        length = read_int32
        return Bytes.empty if length <= 0
        @bytes_remaining -= length
        bytes = Bytes.new(length)
        @io.read_fully(bytes)
        bytes
      end

      private def read_array(&)
        length = read_int32
        return [] of typeof(yield) if length <= 0
        Array.new(length) { yield }
      end

      private def read_nullable_array(&)
        length = read_int32
        return nil if length < 0
        return [] of typeof(yield) if length == 0
        Array.new(length) { yield }
      end

      # Primitive writers
      private def write_string(io : ::IO, str : String)
        io.write_bytes(str.bytesize.to_i16, ::IO::ByteFormat::BigEndian)
        io.write(str.to_slice)
      end

      private def write_nullable_string(io : ::IO, str : String?)
        if str.nil?
          io.write_bytes(-1_i16, ::IO::ByteFormat::BigEndian)
        else
          write_string(io, str)
        end
      end

      private def write_array(io : ::IO, arr, &)
        io.write_bytes(arr.size.to_i32, ::IO::ByteFormat::BigEndian)
        arr.each { |item| yield item }
      end
    end

    # Request types
    abstract struct Request
      getter api_version : Int16
      getter correlation_id : Int32
      getter client_id : String?

      def initialize(@api_version, @correlation_id, @client_id)
      end
    end

    struct ApiVersionsRequest < Request
    end

    struct MetadataRequest < Request
      getter topics : Array(String)?

      def initialize(api_version, correlation_id, client_id, @topics)
        super(api_version, correlation_id, client_id)
      end
    end

    struct ProduceRequest < Request
      getter acks : Int16
      getter timeout_ms : Int32
      getter topic_data : Array(TopicData)

      def initialize(api_version, correlation_id, client_id, @acks, @timeout_ms, @topic_data)
        super(api_version, correlation_id, client_id)
      end
    end

    struct UnknownRequest < Request
      getter api_key : Int16

      def initialize(@api_key, api_version, correlation_id, client_id)
        super(api_version, correlation_id, client_id)
      end
    end

    struct TopicData
      getter name : String
      getter partitions : Array(PartitionData)

      def initialize(@name, @partitions)
      end
    end

    struct PartitionData
      getter partition : Int32
      getter record_set : Bytes

      def initialize(@partition, @record_set)
      end
    end

    # Response types
    abstract struct Response
      getter api_version : Int16
      getter correlation_id : Int32

      def initialize(@api_version, @correlation_id)
      end
    end

    struct ApiVersionsResponse < Response
      getter error_code : Int16
      getter api_versions : Array(ApiVersion)
      getter throttle_time_ms : Int32

      def initialize(api_version, correlation_id, @error_code, @api_versions, @throttle_time_ms = 0)
        super(api_version, correlation_id)
      end
    end

    struct ApiVersion
      getter api_key : Int16
      getter min_version : Int16
      getter max_version : Int16

      def initialize(@api_key, @min_version, @max_version)
      end
    end

    struct MetadataResponse < Response
      getter brokers : Array(BrokerMetadata)
      getter cluster_id : String?
      getter controller_id : Int32
      getter topics : Array(TopicMetadata)

      def initialize(api_version, correlation_id, @brokers, @cluster_id, @controller_id, @topics)
        super(api_version, correlation_id)
      end
    end

    struct BrokerMetadata
      getter node_id : Int32
      getter host : String
      getter port : Int32
      getter rack : String?

      def initialize(@node_id, @host, @port, @rack = nil)
      end
    end

    struct TopicMetadata
      getter error_code : Int16
      getter name : String
      getter? internal : Bool
      getter partitions : Array(PartitionMetadata)

      def initialize(@error_code, @name, @internal, @partitions)
      end
    end

    struct PartitionMetadata
      getter error_code : Int16
      getter partition_index : Int32
      getter leader_id : Int32
      getter replica_nodes : Array(Int32)
      getter isr_nodes : Array(Int32)

      def initialize(@error_code, @partition_index, @leader_id, @replica_nodes, @isr_nodes)
      end
    end

    struct ProduceResponse < Response
      getter responses : Array(TopicProduceResponse)
      getter throttle_time_ms : Int32

      def initialize(api_version, correlation_id, @responses, @throttle_time_ms = 0)
        super(api_version, correlation_id)
      end
    end

    struct TopicProduceResponse
      getter name : String
      getter partition_responses : Array(PartitionProduceResponse)

      def initialize(@name, @partition_responses)
      end
    end

    struct PartitionProduceResponse
      getter index : Int32
      getter error_code : Int16
      getter base_offset : Int64
      getter log_append_time_ms : Int64

      def initialize(@index, @error_code, @base_offset, @log_append_time_ms = -1_i64)
      end
    end
  end
end
