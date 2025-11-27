require "./client"
require "./consts"
require "./protocol"
require "../vhost"
require "../amqp/stream/stream"

module LavinMQ
  module Kafka
    class Broker
      getter vhost : VHost
      @clients = Hash(String, Client).new

      def initialize(@vhost : VHost, @config : Config)
      end

      def add_client(protocol : Protocol, connection_info : ConnectionInfo, client_id : String) : Client
        # Close any existing client with same client_id
        if prev = @clients[client_id]?
          prev.close("New client connected with same client_id")
        end

        client = Client.new(protocol, connection_info, self, client_id)
        @clients[client_id] = client
        @vhost.add_connection(client)
        client
      end

      def remove_client(client : Client)
        @clients.delete(client.client_id)
        @vhost.rm_connection(client)
      end

      # Get stream queue by topic name
      def get_stream(topic : String) : AMQP::Stream?
        @vhost.queues[topic]?.as?(AMQP::Stream)
      end

      # Get or create stream queue for topic
      def get_or_create_stream(topic : String) : AMQP::Stream?
        if existing = get_stream(topic)
          return existing
        end

        return nil unless @config.kafka_auto_create_topics?

        # Create new stream queue
        args = AMQP::Table.new({"x-queue-type" => "stream"})
        @vhost.declare_queue(topic, durable: true, auto_delete: false, arguments: args)
        get_stream(topic)
      rescue ex : LavinMQ::Error::PreconditionFailed
        # Queue exists but is not a stream
        nil
      end

      # List all stream queues as topics
      def list_topics : Array(String)
        @vhost.queues.each_value
          .select { |q| q.is_a?(AMQP::Stream) }
          .map(&.name)
          .to_a
      end

      def close
        @clients.each_value(&.close("Broker closing"))
      end
    end
  end
end
