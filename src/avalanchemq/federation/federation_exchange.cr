require "../exchange"

module AvalancheMQ
  class FederationExchange < TopicExchange
    def type
      "x-federation-upstream"
    end

    def initialize(vhost, name, max_hops = 1)
      arguments = Hash(String, AMQP::Field).new
      arguments["x-internal-purpose"] = "federation"
      arguments["x-max-hops"] = max_hops
      super(vhost, name, durable: true, auto_delete: true, internal: true, arguments: arguments)
    end
  end
end
