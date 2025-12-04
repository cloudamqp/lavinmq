require "socket"
require "../client"
require "../rough_time"
require "./broker"
require "./protocol"
require "./record_batch"
require "./consts"

module LavinMQ
  module Kafka
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter channels, name, client_id, connection_info
      @connected_at = RoughTime.unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @kafka_headers_hash = Hash(String, AMQP::Field).new

      # Stack-allocated structs for tracking streaming response metadata
      record ResponseMetadata,
        topic : String,
        partition_count : Int32,
        base_offset : Int64,
        record_count : Int32,
        error_code : Int16

      rate_stats({"send_oct", "recv_oct"})
      Log = LavinMQ::Log.for "kafka.client"

      def vhost
        @broker.vhost
      end

      def user : Auth::User?
        nil
      end

      def client_name
        "kafka-client-#{@client_id}"
      end

      def initialize(@protocol : Protocol, @connection_info : ConnectionInfo,
                     @broker : Broker, @client_id : String)
        @lock = Mutex.new
        @waitgroup = WaitGroup.new(1)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @connection_info.remote_address.to_s, client_id: @client_id})
        @log = Logger.new(Log, metadata)
        @log.info { "Kafka connection established" }
        spawn read_loop, name: "Kafka read_loop #{@connection_info.remote_address}"
      end

      def handle_request(request : Request)
        case request
        when ApiVersionsRequest
          handle_api_versions(request)
        when MetadataRequest
          handle_metadata(request)
        when ProduceRequest
          handle_produce(request)
        when UnknownRequest
          @log.warn { "Unsupported API key: #{request.api_key}" }
        end
      end

      private def read_loop
        loop do
          @log.trace { "waiting for request" }
          request = @protocol.read_request
          @log.trace { "Received: #{request.class}" }

          handle_request(request)
        end
      rescue ex : ::IO::EOFError
        @log.debug { "Client disconnected" } unless @closed
      rescue ex : Kafka::Protocol::Error
        @log.warn { "Client connection error: #{ex.message}" } unless @closed
      rescue ex : ::IO::Error
        @log.debug { "Client connection error: #{ex.message}" } unless @closed
      rescue ex
        @log.error(exception: ex) { "Read loop error" }
      ensure
        @broker.remove_client(self)
        @waitgroup.done
        close_socket
      end

      private def handle_api_versions(request : ApiVersionsRequest)
        api_versions = SUPPORTED_API_VERSIONS.map do |key, min, max|
          ApiVersion.new(key, min, max)
        end
        # Cap response version to maximum supported version (3)
        # If client requests v4+ but we only support v3, respond with v3 format
        response_version = Math.min(request.api_version, 3_i16)
        response = ApiVersionsResponse.new(
          response_version,
          request.correlation_id,
          ErrorCode::NONE,
          api_versions
        )
        send_response(response)
      end

      private def handle_metadata(request : MetadataRequest)
        topics = request.topics

        # Cap response version to maximum supported version (1)
        # If client requests v2+ but we only support v1, respond with v1 format
        response_version = Math.min(request.api_version, 1_i16)
        @log.debug { "Metadata request v#{request.api_version}, responding with v#{response_version}" }

        # If no topics specified, return all stream queues
        topic_names = if topics.nil? || topics.empty?
                        @broker.list_topics
                      else
                        topics
                      end

        topic_metadata = topic_names.map do |topic_name|
          # Auto-create stream if it doesn't exist
          stream = @broker.get_or_create_stream(topic_name)
          if stream
            partitions = [PartitionMetadata.new(
              error_code: ErrorCode::NONE,
              partition_index: 0,
              leader_id: NODE_ID,
              replica_nodes: [NODE_ID],
              isr_nodes: [NODE_ID]
            )]
            TopicMetadata.new(
              error_code: ErrorCode::NONE,
              name: topic_name,
              internal: false,
              partitions: partitions,
              api_version: response_version
            )
          else
            TopicMetadata.new(
              error_code: ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
              name: topic_name,
              internal: false,
              partitions: [] of PartitionMetadata,
              api_version: response_version
            )
          end
        end

        # Get broker host/port from connection info
        local_addr = @connection_info.local_address
        host = local_addr.address
        port = local_addr.port

        brokers = [BrokerMetadata.new(NODE_ID, host, port, response_version)]

        response = MetadataResponse.new(
          response_version,
          request.correlation_id,
          brokers,
          CLUSTER_ID,
          NODE_ID,
          topic_metadata
        )
        send_response(response)
      end

      private def handle_produce(request : ProduceRequest)
        # Cap response version to maximum supported version (3)
        # If client requests v4+ but we only support v3, respond with v3 format
        response_version = Math.min(request.api_version, 3_i16)

        # Track response metadata per topic (minimal allocation - just topic names as keys)
        response_metadata = Hash(String, {Int64, Int32, Int16}).new

        # Process request in streaming fashion - iterate through parsed request
        request.topic_data.each do |topic_data|
          stream = @broker.get_or_create_stream(topic_data.name)

          unless stream
            # Track error for this topic
            response_metadata[topic_data.name] = {-1_i64, 0, ErrorCode::UNKNOWN_TOPIC_OR_PARTITION}
            next
          end

          # Track base offset for this topic
          base_offset = stream.last_offset + 1
          record_count = 0

          # Create stateful iterator for generator pattern
          partitions = topic_data.partitions
          partition_idx = 0
          batch_idx = 0
          record_idx = 0

          # Batch publish all records for this topic while holding the lock
          # Generator pattern: block is called repeatedly and returns next message or nil
          stream.publish_batch do
            loop do
              break nil if partition_idx >= partitions.size
              partition = partitions[partition_idx]
              batches = partition.record_batches

              break nil if batch_idx >= batches.size
              batch = batches[batch_idx]
              records = batch.records

              if record_idx < records.size
                record = records[record_idx]
                record_idx += 1
                msg = record_to_bytesmessage_direct(topic_data.name, record)
                vhost.event_tick(EventType::ClientPublish)
                record_count += 1
                break msg # Return this message
              else
                record_idx = 0
                batch_idx += 1
                if batch_idx >= batches.size
                  batch_idx = 0
                  partition_idx += 1
                end
                # Continue loop to get next record
              end
            end
          end

          # Store response metadata
          response_metadata[topic_data.name] = {base_offset, record_count, ErrorCode::NONE}
        end

        # Only send response if acks != 0
        return if request.acks == 0

        # Write response directly without intermediate arrays
        write_produce_response_streaming(request.correlation_id, response_version, response_metadata)
      end

      private def write_produce_response_streaming(correlation_id : Int32, api_version : Int16,
                                                   metadata : Hash(String, {Int64, Int32, Int16}))
        # Calculate response size
        size = 4 # correlation_id
        size += 4 # topics array length

        metadata.each do |topic, (_base_offset, _count, _error)|
          size += 2 + topic.bytesize # topic name
          size += 4                   # partitions array length (always 1)
          size += partition_response_size(api_version)
        end

        size += 4 if api_version >= 1 # throttle_time_ms

        @lock.synchronize do
          # Write response
          @protocol.write_bytes(size, IO::ByteFormat::NetworkEndian)
          @protocol.write_bytes(correlation_id, IO::ByteFormat::NetworkEndian)

          # Write topics array count
          @protocol.write_bytes(metadata.size.to_i32, IO::ByteFormat::NetworkEndian)

          # Write each topic response
          metadata.each do |topic, (base_offset, count, error_code)|
            # Write topic name
            @protocol.write_bytes(topic.bytesize.to_i16, IO::ByteFormat::NetworkEndian)
            @protocol.write(topic.to_slice)

            # Write partition responses count (always 1)
            @protocol.write_bytes(1_i32, IO::ByteFormat::NetworkEndian)

            # Write partition response
            @protocol.write_bytes(0_i32, IO::ByteFormat::NetworkEndian) # partition index
            @protocol.write_bytes(error_code, IO::ByteFormat::NetworkEndian)
            @protocol.write_bytes(base_offset, IO::ByteFormat::NetworkEndian)

            if api_version >= 2
              @protocol.write_bytes(RoughTime.unix_ms, IO::ByteFormat::NetworkEndian)
            end
          end

          # Write throttle_time_ms if v1+
          if api_version >= 1
            @protocol.write_bytes(0_i32, IO::ByteFormat::NetworkEndian)
          end

          @protocol.flush
        end
      end

      private def write_topic_produce_response(topic_data : TopicData, api_version : Int16)
        # Write topic name
        @protocol.write_bytes(topic_data.name.bytesize.to_i16, IO::ByteFormat::NetworkEndian)
        @protocol.write(topic_data.name.to_slice)

        # Write partition count
        @protocol.write_bytes(topic_data.partitions.size.to_i32, IO::ByteFormat::NetworkEndian)

        # Write each partition response
        topic_data.partitions.each do |partition|
          response = publish_to_stream(topic_data.name, partition, api_version)
          response.to_io(@protocol, IO::ByteFormat::NetworkEndian)
        end
      end

      private def calculate_produce_response_size(request : ProduceRequest, api_version : Int16) : Int32
        size = 4 # correlation_id
        size += 4 # topics array length

        request.topic_data.each do |topic_data|
          size += 2 + topic_data.name.bytesize # topic name
          size += 4                            # partitions array length
          size += topic_data.partitions.size * partition_response_size(api_version)
        end

        size += 4 if api_version >= 1 # throttle_time_ms
        size
      end

      private def partition_response_size(api_version : Int16) : Int32
        size = 4 + 2 + 8           # index + error_code + base_offset
        size += 8 if api_version >= 2 # log_append_time_ms
        size
      end

      private def publish_to_stream(topic : String, partition : PartitionData, api_version : Int16) : PartitionProduceResponse
        stream = @broker.get_or_create_stream(topic)

        unless stream
          return PartitionProduceResponse.new(
            index: partition.partition,
            error_code: ErrorCode::UNKNOWN_TOPIC_OR_PARTITION,
            base_offset: -1_i64,
            api_version: api_version
          )
        end

        # Get parsed record batches
        batches = partition.record_batches

        if batches.empty?
          return PartitionProduceResponse.new(
            index: 0,
            error_code: ErrorCode::NONE,
            base_offset: stream.last_offset,
            api_version: api_version,
            log_append_time_ms: RoughTime.unix_ms
          )
        end

        base_offset = stream.last_offset + 1

        # Convert each record to LavinMQ message and publish
        batches.each do |batch|
          batch.records.each do |record|
            msg = record_to_message(topic, record)
            stream.publish(msg)
            vhost.event_tick(EventType::ClientPublish)
          end
        end

        PartitionProduceResponse.new(
          index: 0, # Always partition 0
          error_code: ErrorCode::NONE,
          base_offset: base_offset,
          api_version: api_version,
          log_append_time_ms: RoughTime.unix_ms
        )
      rescue ex
        @log.error(exception: ex) { "Error publishing to stream #{topic}" }
        PartitionProduceResponse.new(
          index: 0,
          error_code: ErrorCode::UNKNOWN_SERVER_ERROR,
          base_offset: -1_i64,
          api_version: api_version
        )
      end

      # Direct streaming version - creates BytesMessage with minimal allocations
      private def record_to_bytesmessage_direct(topic : String, record : Record) : BytesMessage
        # Clear and reuse headers hash to avoid allocation
        @kafka_headers_hash.clear

        # Build AMQP headers from Kafka headers
        # Note: record.headers is the Hash from RecordBatch parsing
        record.headers.each do |key, value|
          # Try to decode as string, otherwise store as bytes
          begin
            @kafka_headers_hash[key] = String.new(value)
          rescue
            @kafka_headers_hash[key] = value
          end
        end

        # Store Kafka key in headers if present
        if key = record.key
          @kafka_headers_hash["kafka-key"] = String.new(key)
        end

        # Create Table from hash (Table creation is necessary but hash is reused)
        headers_table = @kafka_headers_hash.empty? ? nil : AMQP::Table.new(@kafka_headers_hash)

        properties = AMQP::Properties.new(
          headers: headers_table,
          timestamp: Time.unix_ms(record.timestamp)
        )

        # Use BytesMessage directly - record.value is already Bytes from socket
        # This is the most direct path: socket bytes -> BytesMessage -> mfile
        BytesMessage.new(
          RoughTime.unix_ms,
          "",
          topic,
          properties,
          record.value.bytesize.to_u64,
          record.value
        )
      end

      private def send_response(response : Response)
        @lock.synchronize do
          @protocol.write_bytes(response, IO::ByteFormat::NetworkEndian)
          @protocol.flush
        end
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              "",
          protocol:          "Kafka",
          client_id:         @client_id,
          name:              @name,
          connected_at:      @connected_at,
          state:             state,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          client_properties: NamedTuple.new,
        }.merge(stats_details)
      end

      def search_match?(value : String) : Bool
        @name.includes?(value) ||
          @client_id.includes?(value)
      end

      def search_match?(value : Regex) : Bool
        value === @name ||
          value === @client_id
      end

      def close(reason = "")
        return if @closed
        @log.info { "Closing connection: #{reason}" }
        @closed = true
        close_socket
        @waitgroup.wait
      end

      def state
        @closed ? "closed" : (@broker.vhost.flow? ? "running" : "flow")
      end

      def force_close
        close_socket
      end

      private def close_socket
        @protocol.close
      rescue ::IO::Error
      end
    end
  end
end
