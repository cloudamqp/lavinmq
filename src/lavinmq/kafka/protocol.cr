require "./consts"
require "./record_batch"

module LavinMQ
  module Kafka
    # Kafka wire protocol IO wrapper
    class Protocol < ::IO
      class Error < ::IO::Error
      end

      class MaxRequestSizeError < Error
      end

      # Maximum request size (128 MiB)
      MAX_REQUEST_SIZE = 128 * 1024 * 1024

      @bytes_remaining : UInt32 = 0_u32

      def initialize(@io : ::IO)
      end

      # Override IO#read to handle bytes_remaining accounting
      def read(slice : Bytes)
        # Limit read to remaining bytes in current request
        if slice.size > @bytes_remaining
          raise MaxRequestSizeError.new("Attempt to read beyond request size")
        end

        bytes_read = @io.read(slice)
        @bytes_remaining -= bytes_read
        bytes_read
      end

      # Override IO#write to forward to underlying IO
      def write(slice : Bytes) : Nil
        @io.write(slice)
      end

      def close
        @io.close
      end

      # Read a complete request from the wire
      def read_request : Request
        # Read size directly (not part of the request body)
        size = @io.read_bytes(Int32, ::IO::ByteFormat::NetworkEndian)

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

        # ApiVersions request header is ALWAYS non-flexible (v1) for backward compatibility
        # even for ApiVersions v3+. This allows clients to negotiate with older servers.
        client_id = read_nullable_string

        case api_key
        when ApiKey::ApiVersions
          # For v3+, there may be additional fields (ClientSoftwareName, ClientSoftwareVersion, TaggedFields)
          # For now, just skip any remaining bytes to avoid parsing errors
          if @bytes_remaining > 0
            self.skip(@bytes_remaining)
          end
          ApiVersionsRequest.new(api_version, correlation_id, client_id)
        when ApiKey::Metadata
          topics = if api_version >= 1
                     read_nullable_array { read_string }
                   else
                     read_array { read_string }
                   end
          MetadataRequest.new(api_version, correlation_id, client_id, topics)
        when ApiKey::Produce
          # Version 3+ includes transactional_id before acks
          transactional_id = if api_version >= 3
                               read_nullable_string
                             else
                               nil
                             end
          acks = read_int16
          timeout_ms = read_int32
          topic_data = read_array do
            topic_name = read_string
            partitions = read_array do
              partition = read_int32
              record_set_length = read_int32
              record_batches = if record_set_length > 0
                                 RecordBatch.parse(self, record_set_length)
                               else
                                 [] of RecordBatch
                               end
              PartitionData.new(partition, record_batches)
            end
            TopicData.new(topic_name, partitions)
          end
          ProduceRequest.new(api_version, correlation_id, client_id, transactional_id, acks, timeout_ms, topic_data)
        else
          UnknownRequest.new(api_key, api_version, correlation_id, client_id)
        end
      end

      # Write a response to the wire
      def write_response(response : Response)
        response.to_io(@io, ::IO::ByteFormat::NetworkEndian)
      end

      def flush
        @io.flush
      end

      # Primitive readers
      private def read_int8 : Int8
        self.read_bytes(Int8, ::IO::ByteFormat::NetworkEndian)
      end

      private def read_int16 : Int16
        self.read_bytes(Int16, ::IO::ByteFormat::NetworkEndian)
      end

      private def read_int32 : Int32
        self.read_bytes(Int32, ::IO::ByteFormat::NetworkEndian)
      end

      private def read_int64 : Int64
        self.read_bytes(Int64, ::IO::ByteFormat::NetworkEndian)
      end

      private def read_string : String
        length = read_int16
        raise Error.new("Invalid string length: #{length}") if length < 0
        self.read_string(length)
      end

      private def read_nullable_string : String?
        length = read_int16
        return nil if length < 0
        self.read_string(length)
      end

      private def read_bytes : Bytes
        length = read_int32
        return Bytes.empty if length <= 0
        bytes = Bytes.new(length)
        self.read_fully(bytes)
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

      # Unsigned varint support for flexible versions (v3+)
      private def read_unsigned_varint : UInt32
        value = 0_u32
        shift = 0
        loop do
          byte = read_int8.to_u8
          value |= ((byte & 0x7F).to_u32 << shift)
          break if (byte & 0x80) == 0
          shift += 7
          raise Error.new("Varint too long") if shift > 28
        end
        value
      end

      # Compact string for flexible versions (varint length + 1, where 0 = null)
      private def read_compact_string : String?
        length = read_unsigned_varint
        return nil if length == 0  # 0 means null
        actual_length = length - 1 # length is encoded as actual + 1
        return "" if actual_length == 0
        # Read bytes directly without length prefix
        bytes = Bytes.new(actual_length)
        read_fully(bytes)
        String.new(bytes)
      end

      # Compact nullable string for flexible versions
      private def read_compact_nullable_string : String?
        read_compact_string
      end

      # Read tagged fields (for flexible versions)
      private def skip_tagged_fields
        num_tags = read_unsigned_varint
        num_tags.times do
          tag = read_unsigned_varint
          size = read_unsigned_varint
          # Skip the tag data
          Bytes.new(size).tap { |bytes| read_fully(bytes) }
        end
      end

      # Primitive writers (kept for backward compatibility if needed)
      private def write_string(io : ::IO, str : String)
        Response.write_string(io, str)
      end

      private def write_nullable_string(io : ::IO, str : String?)
        Response.write_nullable_string(io, str)
      end

      private def write_array(io : ::IO, arr, &)
        io.write_bytes(arr.size.to_i32, ::IO::ByteFormat::NetworkEndian)
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
      getter transactional_id : String?
      getter acks : Int16
      getter timeout_ms : Int32
      getter topic_data : Array(TopicData)

      def initialize(api_version, correlation_id, client_id, @transactional_id, @acks, @timeout_ms, @topic_data)
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
      getter record_batches : Array(RecordBatch)

      def initialize(@partition, @record_batches)
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

      # Unsigned varint helpers for flexible versions
      protected def varint_size(value : UInt32) : Int32
        return 1 if value == 0
        bytes = 0
        v = value
        while v != 0
          bytes += 1
          v >>= 7
        end
        bytes
      end

      protected def write_unsigned_varint(io : ::IO, value : UInt32)
        loop do
          byte = (value & 0x7F).to_u8
          value >>= 7
          if value != 0
            byte |= 0x80
          end
          io.write_byte(byte)
          break if value == 0
        end
      end

      # Primitive writers
      protected def write_string(io : ::IO, str : String)
        io.write_bytes(str.bytesize.to_i16, ::IO::ByteFormat::NetworkEndian)
        io.write(str.to_slice)
      end

      protected def write_nullable_string(io : ::IO, str : String?)
        if str.nil?
          io.write_bytes(-1_i16, ::IO::ByteFormat::NetworkEndian)
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
        size = 4 + 2 # correlation_id + error_code

        if @api_version >= 3
          # Flexible version: compact array (varint length + 1)
          array_length = (@api_versions.size + 1).to_u32
          size += varint_size(array_length)
          # Each ApiVersion: api_key (2) + min_version (2) + max_version (2) + tagged_fields (1)
          size += @api_versions.size * 7
        else
          # Old version: int32 array length + (api_key + min_version + max_version) * count
          size += 4 + (@api_versions.size * 6)
        end

        size += 4 if @api_version >= 1 # throttle_time_ms

        if @api_version >= 3
          size += 1 # Empty tagged fields for response (0x00)
        end

        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(bytesize, format)
        io.write_bytes(@correlation_id, format)
        io.write_bytes(@error_code, format)

        if @api_version >= 3
          # Flexible version: use compact array (unsigned varint length + 1)
          write_unsigned_varint(io, (@api_versions.size + 1).to_u32)
        else
          # Old version: use int32 array length
          io.write_bytes(@api_versions.size.to_i32, format)
        end

        @api_versions.each do |api|
          io.write_bytes(api.api_key, format)
          io.write_bytes(api.min_version, format)
          io.write_bytes(api.max_version, format)
          # In flexible versions, each ApiVersion struct has tagged fields
          if @api_version >= 3
            io.write_byte(0x00_u8) # Empty tagged fields for this ApiVersion
          end
        end

        if @api_version >= 1
          io.write_bytes(@throttle_time_ms, format)
        end

        if @api_version >= 3
          # Write empty tagged fields section for the response
          io.write_byte(0x00_u8)
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
        size += nullable_string_size(@cluster_id) if @api_version >= 2
        size += 4 if @api_version >= 1 # controller_id
        size += array_size(@topics, &.bytesize)
        size
      end

      def to_io(io : IO, format : IO::ByteFormat = IO::ByteFormat::NetworkEndian) : Nil
        io.write_bytes(bytesize, format)
        io.write_bytes(@correlation_id, format)

        # Brokers array
        io.write_bytes(@brokers.size.to_i32, format)
        @brokers.each(&.to_io(io, format))

        write_nullable_string(io, @cluster_id) if @api_version >= 2
        io.write_bytes(@controller_id, format) if @api_version >= 1

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
