require "../observable"
require "./publisher"
require "./consumer"
require "./queue_upstream"
require "./exchange_upstream"

module AvalancheMQ
  class Upstream
    class Link
      include Observer
      getter connected_at

      @publisher : Publisher?
      @consumer : Consumer?

      def initialize(@upstream : QueueUpstream, @federated_q : Queue, @log : Logger)
        @log.progname += " link queue=#{@federated_q.name}:"
        @federated_q.registerObserver(self)
      end

      def on(event, data)
        @log.debug { "event=#{event} data=#{data}" }
        case event
        when :delete, :close
          @upstream.close_link(@federated_q)
        when :rm_consumer
          @upstream.close_link(@federated_q) unless @federated_q.consumer_count > 0
        end
      end

      def start
        @log.debug { "start=#{@federated_q.immediate_delivery?}" }
        return false unless @federated_q.immediate_delivery?
        @consumer.try &.close
        @publisher.try &.close
        @publisher = Publisher.new(@upstream)
        @consumer = Consumer.new(@upstream, @publisher.not_nil!, @federated_q)
        @publisher.not_nil!.start(@consumer.not_nil!)
        @connected_at = Time.utc_now
        @consumer.not_nil!.start
        @log.debug "link stopped"
      end

      def close
        @log.debug "close link"
        @federated_q.unregisterObserver(self)
        @consumer.try &.close
        @publisher.try &.close
      end
    end
  end
end
