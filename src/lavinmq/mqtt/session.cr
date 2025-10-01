require "../amqp/queue/queue"
require "../error"
require "./consts"

module LavinMQ
  module MQTT
    class Session < LavinMQ::AMQP::Queue
      include SortableJSON
      Log = ::LavinMQ::Log.for "mqtt.session"

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @unacked = Hash(UInt16, SegmentPosition).new

        super(@vhost, @name, false, @auto_delete, arguments)

        @log = Logger.new(Log, @metadata)
        spawn deliver_loop, name: "Session#deliver_loop"
      end

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        i = 0
        loop do
          break if @closed
          next @msg_store.empty.when_false.receive? if @msg_store.empty?
          next @consumers_empty.when_false.receive? if @consumers.empty?
          consumer = @consumers.first.as(MQTT::Consumer)
          get_packet do |pub_packet|
            consumer.deliver(pub_packet)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        rescue ex
          @log.error(exception: ex) { "Failed to deliver message in deliver_loop" }
          @consumers.each &.close
          self.client = nil
        end
      end

      def client=(client : MQTT::Client?)
        return if @closed
        @last_get_time = RoughTime.monotonic

        unless clean_session?
          @msg_store_lock.synchronize do
            @unacked.values.each do |sp|
              @msg_store.requeue(sp)
            end
          end
        end
        @unacked.clear

        @consumers.each do |c|
          rm_consumer c
        end

        if c = client
          add_consumer MQTT::Consumer.new(c, self)
        end
        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable?
        !clean_session?
      end

      def subscribe(tf, qos)
        arguments = AMQP::Table.new
        arguments[QOS_HEADER] = qos
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

      def publish(msg : Message) : Bool
        # Do not enqueue messages with QoS 0 if there are no consumers subscribed to the session
        return true if msg.properties.delivery_mode == 0 && @consumers.empty?
        super
      end

      private def find_binding(rk)
        bindings.find { |b| b.binding_key.routing_key == rk }
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, EXCHANGE, rk, arguments || AMQP::Table.new)
      end

      private def get_packet(& : MQTT::Publish -> Nil) : Bool
        raise ClosedError.new if @closed
        loop do
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break
          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            begin
              packet = build_packet(env, nil)
              yield packet
            rescue ex # requeue failed delivery
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
        retained = msg.properties.try &.headers.try &.["mqtt.retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        qos = 1u8 if qos > 1
        dup = qos.zero? ? false : env.redelivered
        MQTT::Publish.new(
          packet_id: packet_id,
          payload: msg.body,
          dup: dup,
          qos: qos,
          retain: retained,
          topic: msg.routing_key
        )
      end

      def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?)
        clear_policy
        Policy.merge_definitions(policy, operator_policy).each do |k, v|
          @log.debug { "Applying policy #{k}: #{v}" }
          case k
          when "max-length"
            unless @max_length.try &.< v.as_i64
              @max_length = v.as_i64
              drop_overflow
            end
          when "max-length-bytes"
            unless @max_length_bytes.try &.< v.as_i64
              @max_length_bytes = v.as_i64
              drop_overflow
            end
          when "overflow"
            @reject_on_overflow ||= v.as_s == "reject-publish"
          end
        end
        @policy = policy
        @operator_policy = operator_policy
      end

      def ack(packet : MQTT::PubAck) : Nil
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
        return nil if @unacked.size == Config.instance.max_inflight_messages
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

      private def handle_arguments
        super
        @effective_args << "x-queue-type"
      end
    end
  end
end
