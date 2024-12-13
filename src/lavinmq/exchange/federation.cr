require "./topic"

module LavinMQ
  module AMQP
    class FederationExchange < TopicExchange
      def type
        "x-federation-upstream"
      end

      def initialize(vhost, name, arguments)
        arguments["x-internal-purpose"] = "federation"
        arguments["x-max-hops"] ||= 1
        super(vhost, name, true, true, true, arguments)
      end
    end
  end
end
