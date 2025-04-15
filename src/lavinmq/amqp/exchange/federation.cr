require "./fanout"

module LavinMQ
  module AMQP
    class FederationExchange < FanoutExchange
      def type
        "x-federation-upstream"
      end

      def initialize(vhost, name, arguments)
        arguments["x-internal-purpose"] = "federation"
        arguments["x-max-hops"] ||= 1
        super(vhost, name, durable: true, auto_delete: true, internal: true, arguments: arguments)
      end
    end
  end
end
