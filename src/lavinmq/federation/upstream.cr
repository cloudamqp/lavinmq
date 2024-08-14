require "uri"
require "./link"

module LavinMQ
  module Federation
    enum AckMode
      OnConfirm
      OnPublish
      NoAck
    end

    class Upstream
      DEFAULT_PREFETCH        = 1000_u16
      DEFAULT_RECONNECT_DELAY =        1
      DEFAULT_ACK_MODE        = AckMode::OnConfirm
      DEFAULT_MAX_HOPS        = 1_i64
      DEFAULT_EXPIRES         = nil
      DEFAULT_MSG_TTL         = nil

      @q_links = Hash(String, QueueLink).new
      @ex_links = Hash(String, ExchangeLink).new
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
      end

      # delete x-federation-upstream exchange on upstream
      # delete queue on upstream
      def stop_link(federated_exchange : Exchange)
        @ex_links.delete(federated_exchange.name).try(&.terminate)
      end

      def stop_link(federated_q : Queue)
        @q_links.delete(federated_q.name).try(&.terminate)
      end

      def links : Array(Link)
        @q_links.values + @ex_links.values
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
        if link = @ex_links[federated_exchange.name]?
          return link
        end
        upstream_exchange = @exchange
        if upstream_exchange.nil? || upstream_exchange.empty?
          upstream_exchange = federated_exchange.name
        end
        upstream_q = "federation: #{upstream_exchange} -> #{System.hostname}:#{vhost.name}:#{federated_exchange.name}"
        link = ExchangeLink.new(self, federated_exchange, upstream_q, upstream_exchange)
        @ex_links[federated_exchange.name] = link
        link.run
        link
      end

      # When federated_q has a consumer the connections are estabished.
      # If all consumers disconnect, the connections are closed.
      # When the policy or the upstream is removed the link is also removed.
      def link(federated_q : Queue) : QueueLink
        if link = @q_links[federated_q.name]?
          return link
        end
        upstream_q = @queue
        if upstream_q.nil? || upstream_q.empty?
          upstream_q = federated_q.name
        end
        link = QueueLink.new(self, federated_q, upstream_q)
        @q_links[federated_q.name] = link
        link.run
        link
      end

      def close
        links.each(&.terminate)
        @ex_links.clear
        @q_links.clear
      end
    end
  end
end
