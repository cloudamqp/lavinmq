require "uri"
require "sync/exclusive"
require "./constants"
require "./link"

module LavinMQ
  module Federation
    class Upstream
      @links : Sync::Exclusive(NamedTuple(queue: Hash(String, QueueLink), exchange: Hash(String, ExchangeLink)))
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
        @links = Sync::Exclusive.new({queue: Hash(String, QueueLink).new, exchange: Hash(String, ExchangeLink).new})
      end

      # delete x-federation-upstream exchange on upstream
      # delete queue on upstream
      def stop_link(federated_exchange : Exchange)
        old = @links.lock(&.[:exchange].delete(federated_exchange.name))
        old.try(&.terminate)
      end

      def stop_link(federated_q : Queue)
        old = @links.lock(&.[:queue].delete(federated_q.name))
        old.try(&.terminate)
      end

      def links : Array(Link)
        @links.lock { |l| l[:queue].values + l[:exchange].values }
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
        new_link = nil
        link = @links.lock do |l|
          if existing = l[:exchange][federated_exchange.name]?
            next existing
          end
          upstream_exchange = @exchange
          if upstream_exchange.nil? || upstream_exchange.empty?
            upstream_exchange = federated_exchange.name
          end
          upstream_q = "federation: #{upstream_exchange} -> #{System.hostname}:#{vhost.name}:#{federated_exchange.name}"
          new_link = ExchangeLink.new(self, federated_exchange, upstream_q, upstream_exchange)
          l[:exchange][federated_exchange.name] = new_link
          new_link
        end
        new_link.try &.run
        link
      end

      # When federated_q has a consumer the connections are estabished.
      # If all consumers disconnect, the connections are closed.
      # When the policy or the upstream is removed the link is also removed.
      def link(federated_q : Queue) : QueueLink
        new_link = nil
        link = @links.lock do |l|
          if existing = l[:queue][federated_q.name]?
            next existing
          end
          upstream_q = @queue
          if upstream_q.nil? || upstream_q.empty?
            upstream_q = federated_q.name
          end
          new_link = QueueLink.new(self, federated_q, upstream_q)
          l[:queue][federated_q.name] = new_link
          new_link
        end
        new_link.try &.run
        link
      end

      def close
        to_terminate = @links.lock do |l|
          result = l[:queue].values.map(&.as(Link)) + l[:exchange].values.map(&.as(Link))
          l[:queue].clear
          l[:exchange].clear
          result
        end
        to_terminate.each(&.terminate)
      end
    end
  end
end
