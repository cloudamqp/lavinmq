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
        response.to_io(@io)
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

      # Primitive writers (kept for backward compatibility if needed)
      private def write_string(io : ::IO, str : String)
        Response.write_string(io, str)
      end

      private def write_nullable_string(io : ::IO, str : String?)
        Response.write_nullable_string(io, str)
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

    # Helper methods for serialization
    module SerializationHelpers
      # Size calculation helpers
      protected def string_size(str : String) : Int32
        2 + str.bytesize # int16 length + bytes
      end

      protected def nullable_string_size(str : String?) : Int32
        str.nil? ? 2 : string_size(str)
      end

      protected def array_size(arr, &) : Int32
        4 + arr.sum { |item| yield item } # int32 length + sum of item sizes
      end

      # Primitive writers
      protected def write_string(io : ::IO, str : String)
        io.write_bytes(str.bytesize.to_i16, ::IO::ByteFormat::BigEndian)
        io.write(str.to_slice)
      end

      protected def write_nullable_string(io : ::IO, str : String?)
        if str.nil?
          io.write_bytes(-1_i16, ::IO::ByteFormat::BigEndian)
        else
          write_string(io, str)
        end
      end
    end

    # Response types
    abstract struct Response
      include SerializationHelpers

      getter api_version : Int16
      getter correlation_id : Int32

      def initialize(@api_version, @correlation_id)
      end

      abstract def bytesize : Int32
      abstract def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
    end

    struct ApiVersionsResponse < Response
      getter error_code : Int16
      getter api_versions : Array(ApiVersion)
      getter throttle_time_ms : Int32

      def initialize(api_version, correlation_id, @error_code, @api_versions, @throttle_time_ms = 0)
        super(api_version, correlation_id)
      end

      def bytesize : Int32
        size = 4 + 2                         # correlation_id + error_code
        size += 4 + (@api_versions.size * 6) # array length + (api_key + min_version + max_version) * count
        size += 4 if @api_version >= 1       # throttle_time_ms
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(bytesize, format)
        io.write_bytes(@correlation_id, format)
        io.write_bytes(@error_code, format)
        io.write_bytes(@api_versions.size.to_i32, format)
        @api_versions.each do |api|
          io.write_bytes(api.api_key, format)
          io.write_bytes(api.min_version, format)
          io.write_bytes(api.max_version, format)
        end
        if @api_version >= 1
          io.write_bytes(@throttle_time_ms, format)
        end
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

      def bytesize : Int32
        size = 4 # correlation_id
        size += array_size(@brokers, &.bytesize)
        if @api_version >= 1
          size += nullable_string_size(@cluster_id)
          size += 4 # controller_id
        end
        size += array_size(@topics, &.bytesize)
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(bytesize, format)
        io.write_bytes(@correlation_id, format)

        # Brokers array
        io.write_bytes(@brokers.size.to_i32, format)
        @brokers.each(&.to_io(io, format))

        if @api_version >= 1
          write_nullable_string(io, @cluster_id)
          io.write_bytes(@controller_id, format)
        end

        # Topics array
        io.write_bytes(@topics.size.to_i32, format)
        @topics.each(&.to_io(io, format))
      end
    end

    struct BrokerMetadata
      include SerializationHelpers

      getter node_id : Int32
      getter host : String
      getter port : Int32
      getter rack : String?
      getter api_version : Int16

      def initialize(@node_id, @host, @port, @api_version, @rack = nil)
      end

      def bytesize : Int32
        size = 4 + string_size(@host) + 4 # node_id + host + port
        size += nullable_string_size(@rack) if @api_version >= 1
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(@node_id, format)
        write_string(io, @host)
        io.write_bytes(@port, format)
        write_nullable_string(io, @rack) if @api_version >= 1
      end
    end

    struct TopicMetadata
      include SerializationHelpers

      getter error_code : Int16
      getter name : String
      getter? internal : Bool
      getter partitions : Array(PartitionMetadata)
      getter api_version : Int16

      def initialize(@error_code, @name, @internal, @partitions, @api_version)
      end

      def bytesize : Int32
        size = 2 + string_size(@name)  # error_code + name
        size += 1 if @api_version >= 1 # internal flag
        size += array_size(@partitions, &.bytesize)
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(@error_code, format)
        write_string(io, @name)
        io.write_byte(@internal ? 1u8 : 0u8) if @api_version >= 1
        io.write_bytes(@partitions.size.to_i32, format)
        @partitions.each(&.to_io(io, format))
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

      def bytesize : Int32
        size = 2 + 4 + 4                      # error_code + partition_index + leader_id
        size += 4 + (@replica_nodes.size * 4) # replica_nodes array
        size += 4 + (@isr_nodes.size * 4)     # isr_nodes array
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(@error_code, format)
        io.write_bytes(@partition_index, format)
        io.write_bytes(@leader_id, format)
        io.write_bytes(@replica_nodes.size.to_i32, format)
        @replica_nodes.each { |node| io.write_bytes(node, format) }
        io.write_bytes(@isr_nodes.size.to_i32, format)
        @isr_nodes.each { |node| io.write_bytes(node, format) }
      end
    end

    struct ProduceResponse < Response
      getter responses : Array(TopicProduceResponse)
      getter throttle_time_ms : Int32

      def initialize(api_version, correlation_id, @responses, @throttle_time_ms = 0)
        super(api_version, correlation_id)
      end

      def bytesize : Int32
        size = 4 # correlation_id
        size += array_size(@responses, &.bytesize)
        size += 4 if @api_version >= 1 # throttle_time_ms
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(bytesize, format)
        io.write_bytes(@correlation_id, format)
        io.write_bytes(@responses.size.to_i32, format)
        @responses.each(&.to_io(io, format))
        if @api_version >= 1
          io.write_bytes(@throttle_time_ms, format)
        end
      end
    end

    struct TopicProduceResponse
      include SerializationHelpers

      getter name : String
      getter partition_responses : Array(PartitionProduceResponse)
      getter api_version : Int16

      def initialize(@name, @partition_responses, @api_version)
      end

      def bytesize : Int32
        size = string_size(@name)
        size += array_size(@partition_responses, &.bytesize)
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        write_string(io, @name)
        io.write_bytes(@partition_responses.size.to_i32, format)
        @partition_responses.each(&.to_io(io, format))
      end
    end

    struct PartitionProduceResponse
      getter index : Int32
      getter error_code : Int16
      getter base_offset : Int64
      getter log_append_time_ms : Int64
      getter api_version : Int16

      def initialize(@index, @error_code, @base_offset, @api_version, @log_append_time_ms = -1_i64)
      end

      def bytesize : Int32
        size = 4 + 2 + 8               # index + error_code + base_offset
        size += 8 if @api_version >= 2 # log_append_time_ms
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(@index, format)
        io.write_bytes(@error_code, format)
        io.write_bytes(@base_offset, format)
        if @api_version >= 2
          io.write_bytes(@log_append_time_ms, format)
        end
      end
    end
  end
end
