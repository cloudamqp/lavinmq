module LavinMQ
  module MQTT
    class Session < Queue
      @clean_session : Bool = false
      getter clean_session

      def initialize(@vhost : VHost,
                     @name : String,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @unacked = Deque(SegmentPosition).new
        super(@vhost, @name, false, @auto_delete, arguments)
        spawn deliver_loop, name: "Consumer deliver loop", same_thread: true
      end

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        i = 0
        loop do
          break if consumers.empty?
          consume_get(consumers.first) do |env|
            consumers.first.deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue ex
        puts "deliver loop exiting: #{ex.inspect_with_backtrace}"
      end

      def client=(client : MQTT::Client?)
        return if @closed
        @last_get_time = RoughTime.monotonic
        consumers.each do |c|
          c.close
          rm_consumer c
        end

        @msg_store_lock.synchronize do
          @unacked.each do |sp|
            @msg_store.requeue(sp)
          end
        end
        @unacked.clear

        if c = client
          @consumers << MqttConsumer.new(c, self)
          spawn deliver_loop, name: "Consumer deliver loop", same_thread: true
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
        @vhost.unbind_queue(@name, "mqtt.default", rk, arguments || AMQP::Table.new)
      end

      private def get(no_ack : Bool, & : Envelope -> Nil) : Bool
        raise ClosedError.new if @closed
        loop do # retry if msg expired or deliver limit hit
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break

          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            pp "no ack"
            begin
              yield env # deliver the message
            rescue ex   # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              raise ex
            end
            delete_message(sp)
          else
            env.message.properties.message_id = next_id.to_s
            mark_unacked(sp) do
              yield env # deliver the message
              @unacked << sp
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

      def ack(sp : SegmentPosition) : Nil
        # TODO: maybe risky to not have lock around this
        pp "Acking?"
        @unacked.delete sp
        super sp
      end

      private def message_expire_loop; end

      private def queue_expire_loop; end

      private def next_id : UInt16?
        @count += 1u16

        # return nil if @unacked.size == @max_inflight
        # start_id = @packet_id
        # next_id : UInt16 = start_id + 1
        # while @unacked.has_key?(next_id)
        #   if next_id == 65_535
        #     next_id = 1
        #   else
        #     next_id += 1
        #   end
        #   if next_id == start_id
        #     return nil
        #   end
        # end
        # @packet_id = next_id
        # next_id
      end
    end
  end
end
