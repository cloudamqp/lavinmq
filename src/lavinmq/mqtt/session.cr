require "../amqp/queue/queue"
require "../error"
require "./consts"

module LavinMQ
  module MQTT
    class Session < LavinMQ::AMQP::Queue
      include SortableJSON
      Log = ::LavinMQ::Log.for "mqtt.session"

      @clean_session : Bool = false
      getter clean_session
      getter max_inflight_messages : UInt16? = Config.instance.max_inflight_messages

      def initialize(@vhost : VHost,
                     @name : String,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @unacked = Hash(UInt16, SegmentPosition).new

        super(@vhost, @name, false, @auto_delete, arguments)

        @log = Logger.new(Log, @metadata)
        spawn deliver_loop, name: "Session#deliver_loop", same_thread: true
      end

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        i = 0
        loop do
          break if @closed
          if @msg_store.empty? || @consumers.empty?
            Channel.receive_first(@msg_store.empty_change, @consumers_empty_change)
            next
          end
          consumer = consumers.first.as(MQTT::Consumer)
          get_packet(false) do |pub_packet|
            consumer.deliver(pub_packet)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        rescue ::IO::Error
        rescue ex
          @log.error(exception: ex) { "Unexpected error in deliver loop" }
        end
      rescue ::Channel::ClosedError
        return
      rescue ex
        @log.error(exception: ex) { "deliver loop exited unexpectedly" }
      end

      def client=(client : MQTT::Client?)
        return if @closed
        @last_get_time = RoughTime.monotonic
        consumers.each do |c|
          c.close
          rm_consumer c
        end

        @msg_store_lock.synchronize do
          @unacked.values.each do |sp|
            @msg_store.requeue(sp)
          end
        end
        @unacked.clear

        if c = client
          add_consumer MQTT::Consumer.new(c, self)
        end
        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable?
        !clean_session?
      end

      def unacked_messages
        Iterator(UnackedMessage).empty
      end

      def subscribe(tf, qos)
        arguments = AMQP::Table.new({QOS_HEADER: qos})
        if binding = find_binding(tf)
          return if binding.binding_key.arguments == arguments
          unbind(tf, binding.binding_key.arguments)
        end
        @vhost.bind_queue(@name, EXCHANGE, tf, arguments)
      end

      def unsubscribe(tf)
        if binding = find_binding(tf)
          unbind(tf, binding.binding_key.arguments)
        end
      end

      private def find_binding(rk)
        bindings.find { |b| b.binding_key.routing_key == rk }
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, EXCHANGE, rk, arguments || AMQP::Table.new)
      end

      private def get_packet(no_ack : Bool, & : MQTT::Publish -> Nil) : Bool
        raise ClosedError.new if @closed
        loop do
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break
          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            packet = build_packet(env, nil)
            begin
              yield packet
            rescue ex
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              raise ex
            end
            delete_message(sp)
          else
            id = next_id
            return false unless id
            packet = build_packet(env, id)
            mark_unacked(sp) do
              yield packet
              @unacked[id] = sp
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

      def build_packet(env, packet_id) : MQTT::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.["x-mqtt-retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        MQTT::Publish.new(
          packet_id: packet_id,
          payload: msg.body,
          dup: env.redelivered,
          qos: qos,
          retain: retained,
          topic: msg.routing_key
        )
      end

      def ack(packet : MQTT::PubAck) : Nil
        # TODO: maybe risky to not have lock around this
        id = packet.packet_id
        sp = @unacked[id]
        @unacked.delete id
        super sp
      rescue
        raise ::IO::Error.new("Could not acknowledge package with id: #{id}")
      end

      private def message_expire_loop; end

      private def queue_expire_loop; end

      private def next_id : UInt16?
        return nil if @unacked.size == max_inflight_messages
        start_id = @count
        next_id : UInt16 = start_id &+ 1_u16
        while @unacked.has_key?(next_id)
          next_id &+= 1u16
          next_id = 1u16 if next_id == 0
          return nil if next_id == start_id
        end
        @count = next_id
        next_id
      end
    end
  end
end
