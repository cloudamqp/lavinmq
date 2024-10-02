module LavinMQ
  module MQTT
    class Session < Queue
      @clean_session : Bool = false
      getter clean_session

      def initialize(@vhost : VHost,
                     @name : String,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super(@vhost, @name, false, @auto_delete, arguments)
      end

      def clean_session?
        @auto_delete
      end

      def client=(client : MQTT::Client?)
        return if @closed
          @last_get_time = RoughTime.monotonic
          @consumers_lock.synchronize do
            consumers.each &.close
            @consumers.clear
            if c = client
              @consumers << MqttConsumer.new(c, self)
            end
        end
        @log.debug { "Setting MQTT client" }
      end

      def durable?
        !clean_session?
      end

      def subscribe(rk, qos)
        arguments = AMQP::Table.new({"x-mqtt-qos": qos})
        if binding = find_binding(rk)
          return if binding.binding_key.arguments == arguments
          unbind(rk, binding.binding_key.arguments)
        end
        @vhost.bind_queue(@name, "mqtt.default", rk, arguments)
      end

      def unsubscribe(rk)
        if binding = find_binding(rk)
          unbind(rk, binding.binding_key.arguments)
        end
      end

      private def find_binding(rk)
        bindings.find { |b| b.binding_key.routing_key == rk }
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, "amq.topic", rk, arguments || AMQP::Table.new)
      end
    end
  end
end
