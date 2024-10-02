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

      private def get(no_ack : Bool, & : Envelope -> Nil) : Bool
        raise ClosedError.new if @closed
        loop do # retry if msg expired or deliver limit hit
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break

          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            begin
              yield env # deliver the message
            rescue ex   # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              raise ex
            end
            delete_message(sp)
          else
            mark_unacked(sp) do
              yield env # deliver the message
            end
          end
          return true
        end
        false
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ClosedError.new(cause: ex)
      end

      private def message_expire_loop
      end

      private def queue_expire_loop
      end
    end
  end
end
