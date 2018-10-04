require "./upstream"

module AvalancheMQ
  class ExchangeUpstream < Upstream
    property exchange, max_hops, expires, msg_ttl

    @exchange : String?

    def initialize(vhost : VHost, name : String, uri : String, @exchange = nil,
                   @max_hops = DEFAULT_MAX_HOPS, @expires = DEFAULT_EXPIRES,
                   @msg_ttl = DEFAULT_MSG_TTL, prefetch = DEFAULT_PREFETCH,
                   reconnect_delay = DEFUALT_RECONNECT_DELAY, ack_mode = DEFAULT_ACK_MODE)
      super(vhost, name, uri, prefetch.to_u16, reconnect_delay, ack_mode)
    end

    def stop_link(federated_exchange : Exchange)
      @links.delete(federated_exchange.name).try(&.close)
      # delete x-federation-upstream exchange on upstream
      # delete queue on upstream
    end

    def link(federated_exchange : Exchange)
      # declare queue on upstream
      # consume queue and publish to downstream exchange
      # declare upstream exchange (passive)
      # declare x-federation-upstream exchange on upstream
      # bind x-federation-upstream exchange to queue
      # get bindings for downstream exchange
      # add bindings from upstream exchange to x-federation-upstream exchange

      # keep downstream exchange bindings reflected on x-federation-upstream exchange
    end
  end
end
