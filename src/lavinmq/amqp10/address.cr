require "uri"

module LavinMQ::AMQP10
  record PublishAddress, exchange : String, routing_key : String

  # The /queues/<queue> and /exchanges/<exchange>[/<routing-key>] address
  # schemes, with percent-encoded components.
  module Address
    extend self

    def parse_target(address : String) : PublishAddress?
      if queue = parse_source(address)
        PublishAddress.new("", queue)
      elsif exchange = parse_exchange(address)
        PublishAddress.new(*exchange)
      end
    end

    # The queue a /queues/ address names
    def parse_source(address : String) : String?
      encoded = address.lchop?("/queues/") || return
      return if encoded.empty? || encoded.includes?('/')
      URI.decode(encoded)
    end

    # The exchange and routing key (empty if none) an /exchanges/ address
    # names, published to by a target and subscribed to by a source
    def parse_exchange(address : String) : Tuple(String, String)?
      rest = address.lchop?("/exchanges/") || return
      exchange, _, routing_key = rest.partition('/')
      return if exchange.empty?
      {URI.decode(exchange), URI.decode(routing_key)}
    end
  end
end
