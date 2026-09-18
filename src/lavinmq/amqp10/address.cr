require "uri"

module LavinMQ::AMQP10
  record PublishAddress, exchange : String, routing_key : String

  module Address
    extend self

    def parse_target(address : String) : PublishAddress?
      if component = queue_component(address)
        PublishAddress.new("", component)
      elsif exchange = exchange_component(address)
        PublishAddress.new(exchange[0], exchange[1])
      end
    end

    def parse_source(address : String) : String?
      queue_component(address)
    end

    private def queue_component(address : String) : String?
      prefix = "/queues/"
      return unless address.starts_with?(prefix)
      encoded = address[prefix.bytesize..]?
      return if encoded.nil? || encoded.empty? || encoded.includes?('/')
      percent_decode(encoded)
    end

    private def exchange_component(address : String) : Tuple(String, String)?
      prefix = "/exchanges/"
      return unless address.starts_with?(prefix)
      rest = address[prefix.bytesize..]?
      return if rest.nil? || rest.empty?
      slash = rest.index('/')
      if slash
        exchange = rest[0, slash]
        routing_key = rest[(slash + 1)..]
      else
        exchange = rest
        routing_key = ""
      end
      return if exchange.empty?
      {percent_decode(exchange), percent_decode(routing_key)}
    end

    private def percent_decode(value : String) : String
      URI.decode(value)
    end
  end
end
