require "uri"
require "sync/exclusive"
require "./constants"
require "./link"

module LavinMQ
  module Federation
    class Upstream
      @links : Sync::Exclusive({Hash(String, QueueLink), Hash(String, ExchangeLink)})
      @queue : String?
      @exchange : String?
      @expires : Int64?
      @msg_ttl : Int64?
      getter name, log, vhost, type, consumer_tag
      property uri, prefetch, reconnect_delay, ack_mode, exchange,
        max_hops, expires, msg_ttl, queue

      def initialize(@vhost : VHost, @name : String, raw_uri : String,
                     @exchange = nil, @queue = nil,
                     @ack_mode = DEFAULT_ACK_MODE, @expires = DEFAULT_EXPIRES,
                     @max_hops = DEFAULT_MAX_HOPS, @msg_ttl = DEFAULT_MSG_TTL,
                     @prefetch = DEFAULT_PREFETCH, @reconnect_delay = DEFAULT_RECONNECT_DELAY,
                     consumer_tag = nil)
        @consumer_tag = "federation-link-#{@name}"
        @uri = URI.parse(raw_uri)
        @links = Sync::Exclusive.new({Hash(String, QueueLink).new, Hash(String, ExchangeLink).new})
      end

      # delete x-federation-upstream exchange on upstream
      # delete queue on upstream
      def stop_link(federated_exchange : Exchange)
        old = @links.lock { |state| state[1].delete(federated_exchange.name) }
        old.try(&.terminate)
      end

      def stop_link(federated_q : Queue)
        old = @links.lock { |state| state[0].delete(federated_q.name) }
        old.try(&.terminate)
      end

      def links : Array(Link)
        @links.lock { |state| state[0].values + state[1].values }
      end

      # declare queue on upstream
      # consume queue and publish to downstream exchange
      # declare upstream exchange (passive)
      # declare x-federation-upstream exchange on upstream
      # bind x-federation-upstream exchange to queue
      # get bindings for downstream exchange
      # add bindings from upstream exchange to x-federation-upstream exchange
      # keep downstream exchange bindings reflected on x-federation-upstream exchange
      def link(federated_exchange : Exchange) : ExchangeLink
        existing = @links.lock { |state| state[1][federated_exchange.name]? }
        return existing if existing
        upstream_exchange = @exchange
        if upstream_exchange.nil? || upstream_exchange.empty?
          upstream_exchange = federated_exchange.name
        end
        upstream_q = "federation: #{upstream_exchange} -> #{System.hostname}:#{vhost.name}:#{federated_exchange.name}"
        link = ExchangeLink.new(self, federated_exchange, upstream_q, upstream_exchange)
        @links.lock { |state| state[1][federated_exchange.name] = link }
        link.run
        link
      end

      # When federated_q has a consumer the connections are estabished.
      # If all consumers disconnect, the connections are closed.
      # When the policy or the upstream is removed the link is also removed.
      def link(federated_q : Queue) : QueueLink
        existing = @links.lock { |state| state[0][federated_q.name]? }
        return existing if existing
        upstream_q = @queue
        if upstream_q.nil? || upstream_q.empty?
          upstream_q = federated_q.name
        end
        link = QueueLink.new(self, federated_q, upstream_q)
        @links.lock { |state| state[0][federated_q.name] = link }
        link.run
        link
      end

      def close
        to_terminate = @links.lock do |state|
          q_links, ex_links = state
          result = q_links.values.map(&.as(Link)) + ex_links.values.map(&.as(Link))
          q_links.clear
          ex_links.clear
          result
        end
        to_terminate.each(&.terminate)
      end
    end
  end
end
