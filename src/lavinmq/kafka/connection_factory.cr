require "log"
require "socket"
require "./protocol"
require "./brokers"
require "../client/connection_factory"

module LavinMQ
  module Kafka
    class ConnectionFactory < LavinMQ::ConnectionFactory
      Log = LavinMQ::Log.for "kafka.connection_factory"

      def initialize(@brokers : Brokers, @config : Config)
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
        logger = Logger.new(Log, metadata)

        protocol = Protocol.new(socket)

        # Read first request - should be ApiVersions
        request = protocol.read_request
        logger.trace { "recv #{request.class}" }

        case request
        when ApiVersionsRequest
          # Respond with supported API versions
          api_versions = SUPPORTED_API_VERSIONS.map do |key, min, max|
            ApiVersion.new(key, min, max)
          end
          response = ApiVersionsResponse.new(
            request.api_version,
            request.correlation_id,
            ErrorCode::NONE,
            api_versions
          )
          protocol.write_response(response)
          protocol.flush

          # Continue with connection handling
          handle_connection(protocol, connection_info, request.client_id, logger)
        when MetadataRequest
          # Some clients skip ApiVersions and go straight to Metadata
          handle_connection(protocol, connection_info, request.client_id, logger, first_request: request)
        else
          logger.warn { "Unexpected first request: #{request.class}" }
          socket.close
        end
      rescue ex : ::IO::EOFError
        logger.debug { "Client disconnected during handshake" } if logger
        socket.close
      rescue ex
        Log.warn { "Connection error: #{ex.inspect}" }
        socket.close rescue nil
      end

      private def handle_connection(protocol : Protocol, connection_info : ConnectionInfo,
                                    client_id : String?, logger : Logger,
                                    first_request : Request? = nil)
        # Use default vhost for now (no auth)
        vhost = @config.default_kafka_vhost
        broker = @brokers[vhost]?
        unless broker
          logger.warn { "Vhost '#{vhost}' not found" }
          return nil
        end

        # Generate client_id if not provided
        actual_client_id = client_id || "kafka-#{connection_info.remote_address}"

        # Create client and let it handle the connection
        client = broker.add_client(protocol, connection_info, actual_client_id)

        # If we have a pending request (e.g., MetadataRequest), pass it to the client
        if first_request
          client.handle_request(first_request)
        end

        client
      end
    end
  end
end
