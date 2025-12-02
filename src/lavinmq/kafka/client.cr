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

        request.topic_data
        responses = request.topic_data.map do |topic_data|
          partition_responses = topic_data.partitions.map do |partition|
            publish_to_stream(topic_data.name, partition, response_version)
          end
          TopicProduceResponse.new(topic_data.name, partition_responses, response_version)
        end

        # Only send response if acks != 0
        if request.acks != 0
          response = ProduceResponse.new(
            response_version,
            request.correlation_id,
            responses
          )
          send_response(response)
        end
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

      private def record_to_message(topic : String, record : Record) : Message
        # Build AMQP headers from Kafka headers
        headers = AMQP::Table.new

        record.headers.each do |key, value|
          # Try to decode as string, otherwise store as bytes
          begin
            headers[key] = String.new(value)
          rescue
            headers[key] = value
          end
        end

        # Store Kafka key in headers if present
        if key = record.key
          headers["kafka-key"] = String.new(key)
        end

        properties = AMQP::Properties.new(
          headers: headers.empty? ? nil : headers,
          timestamp: Time.unix_ms(record.timestamp)
        )

        body_io = ::IO::Memory.new(record.value)

        Message.new(
          RoughTime.unix_ms,
          "",
          topic,
          properties,
          record.value.bytesize.to_u64,
          body_io
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
