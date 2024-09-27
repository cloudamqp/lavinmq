module LavinMQ
  module MQTT
    class Session < Queue
        @clean_session : Bool = false
        @subscriptions : Int32 = 0
        getter clean_session

      def initialize(@vhost : VHost,
                     @name : String,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super(@vhost, @name, false, @auto_delete, arguments)
      end

      def clean_session?; @auto_delete; end
      def durable?; !clean_session?; end

      # TODO: "amq.tocpic" is hardcoded, should be the mqtt-exchange when that is finished
      def subscribe(rk, qos)
        arguments = AMQP::Table.new({"x-mqtt-qos": qos})
        if binding = bindings.find { |b| b.binding_key.routing_key == rk }
          return if binding.binding_key.arguments == arguments
          @vhost.unbind_queue(@name, "amq.topic", rk, binding.binding_key.arguments || AMQP::Table.new)
        end
        @vhost.bind_queue(@name, "amq.topic", rk, arguments)
      end

      def unsubscribe(rk)
        # unbind session from the exchange
        # decrease @subscriptions by 1
        # if subscriptions is empty, delete the session(do that from broker?)
      end
    end
  end
end
