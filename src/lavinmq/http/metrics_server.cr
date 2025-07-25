require "http/server"
require "./controller/prometheus"

module LavinMQ
  module HTTP
    class MetricsServer
      Log = LavinMQ::Log.for "http.metrics_server"

      def initialize(@amqp_server : LavinMQ::Server?)
        @prometheus_controller = PrometheusController.new(@amqp_server)
        @http = ::HTTP::Server.new do |context|
          case context.request.path
          when "/metrics"
            serve_metrics(context)
          when "/metrics/detailed"
            serve_detailed_metrics(context)
          else
            context.response.status_code = 404
            context.response.content_type = "text/plain"
            context.response.print "Not Found"
          end
        end
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        Log.info { "Metrics server bound to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def close
        @http.try &.close
      end

      private def serve_metrics(context)
        context.response.content_type = "text/plain"
        prefix = context.request.query_params["prefix"]? || "lavinmq"
        if prefix.bytesize > 20
          context.response.status_code = 400
          context.response.print "Prefix too long (max 20 characters)"
          return
        end
        
        # Get all vhosts - no user-based filtering for unauthenticated endpoint
        vhosts = @amqp_server ? @amqp_server.vhosts.values.to_a : [] of LavinMQ::VHost

        @prometheus_controller.report(context.response) do
          writer = PrometheusWriter.new(context.response, prefix)
          @prometheus_controller.overview_broker_metrics(vhosts, writer)
          @prometheus_controller.overview_queue_metrics(vhosts, writer)
          @prometheus_controller.custom_metrics(writer)
          @prometheus_controller.gc_metrics(writer)
          @prometheus_controller.global_metrics(writer)
        end
      end

      private def serve_detailed_metrics(context)
        context.response.content_type = "text/plain"
        prefix = context.request.query_params["prefix"]? || "lavinmq"
        if prefix.bytesize > 20
          context.response.status_code = 400
          context.response.print "Prefix too long (max 20 characters)"
          return
        end
        
        families = context.request.query_params.fetch_all("family")
        # Get all vhosts - no user-based filtering for unauthenticated endpoint
        vhosts = @amqp_server ? @amqp_server.vhosts.values.to_a : [] of LavinMQ::VHost

        @prometheus_controller.report(context.response) do
          writer = PrometheusWriter.new(context.response, prefix)
          families.each do |family|
            case family
            when "connection_churn_metrics"
              @prometheus_controller.detailed_connection_churn_metrics(vhosts, writer)
            when "queue_coarse_metrics"
              @prometheus_controller.detailed_queue_coarse_metrics(vhosts, writer)
            when "queue_consumer_count"
              @prometheus_controller.detailed_queue_consumer_count(vhosts, writer)
            when "connection_coarse_metrics", "connection_metrics"
              @prometheus_controller.detailed_connection_coarse_metrics(vhosts, writer)
            when "channel_metrics"
              @prometheus_controller.detailed_channel_metrics(vhosts, writer)
            when "exchange_metrics"
              @prometheus_controller.detailed_exchange_metrics(vhosts, writer)
            end
          end
        end
      end
    end
  end
end