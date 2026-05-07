require "digest/sha1"
require "../amqp/queue/queue"
require "../error"
require "../sortable_json"
require "./client"
require "../policy"
require "../queue_stats"
require "./consts"

module LavinMQ
  module MQTT
    class Session
      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      Log = ::LavinMQ::Log.for "mqtt.session"

      getter name : String
      getter vhost : VHost

      def consumer_count : UInt32
        @client.nil? ? 0u32 : 1u32
      end

      def message_count : UInt32
        @msg_store.size.to_u32
      end

      def immediate_delivery? : Bool
        false
      end

      getter? auto_delete

      def exclusive? : Bool
        false
      end

      def arguments : AMQP::Table
        AMQP::Table.new
      end

      def close : Bool
        return false if @closed
        @closed = true
        @msg_store_lock.synchronize do
          @msg_store.close
        end
        true
      end

      def delete : Bool
        return false if @deleted
        @deleted = true
        close
        @msg_store_lock.synchronize do
          @msg_store.delete
        end
        @vhost.delete_queue(@name)
        true
      end

      @effective_args = Array(String).new
      @max_length : Int64? = nil
      @max_length_bytes : Int64? = nil
      @reject_on_overflow = false
      @msg_store_lock = Mutex.new(:reentrant)
      @msg_store : MessageStore
      @metadata : ::Log::Metadata
      @closed = false
      @deleted = false
      @client : MQTT::Client? = nil
      @has_client = BoolChannel.new(false)

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @unacked = Hash(UInt16, SegmentPosition).new
        @has_capacity = BoolChannel.new(true)

        @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
        data_dir = File.join(
          durable? ? @vhost.data_dir : File.join(@vhost.data_dir, "transient"),
          Digest::SHA1.hexdigest(@name)
        )
        Dir.mkdir_p(data_dir) unless Dir.exists?(data_dir)
        replicator = durable? ? @vhost.@replicator : nil
        @msg_store = MessageStore.new(data_dir, replicator, durable?, metadata: @metadata)

        @log = Logger.new(Log, @metadata)
        spawn deliver_loop, name: "Session#deliver_loop"
      end

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        loop do
          break if @closed
          next @msg_store.empty.when_false.receive? if @msg_store.empty?
          client = @client
          next @has_client.when_true.receive? if client.nil?
          next @has_capacity.when_true.receive? unless @has_capacity.value
          get_packet do |pub_packet, bytesize|
            client.send(pub_packet)
            delivered_bytes &+= bytesize
          end
          if delivered_bytes > Config.instance.yield_each_delivered_bytes
            delivered_bytes = 0
            Fiber.yield
          end
        rescue ex
          @log.error(exception: ex) { "Failed to deliver message in deliver_loop" }
          @client.try &.close("Server force closed client")
          self.client = nil
        end
      end

      def client : MQTT::Client?
        @client
      end

      def client=(client : MQTT::Client?)
        return if @closed
        @last_get_time = RoughTime.instant

        unless clean_session?
          @msg_store_lock.synchronize do
            @unacked.values.each do |sp|
              @msg_store.requeue(sp)
            end
          end
        end
        @unacked.clear
        @unacked_count.set(0, :release)
        @unacked_bytesize.set(0, :release)
        @has_capacity.set(true)

        @client = client
        @has_client.set(!client.nil?)

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
        return true if msg.properties.delivery_mode == 0 && @client.nil?
        return false if @deleted || @closed
        @msg_store_lock.synchronize do
          @msg_store.push(msg)
          drop_overflow
        end
        @publish_count.add(1, :relaxed)
        true
      end

      def bindings
        @vhost.queue_bindings(self)
      end

      private def find_binding(rk)
        bindings.find { |b| b.binding_key.routing_key == rk }
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, EXCHANGE, rk, arguments || AMQP::Table.new)
      end

      private def get_packet(& : MQTT::Publish, UInt32 -> Nil) : Bool
        raise AMQP::Queue::ClosedError.new if @closed
        loop do
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break
          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            begin
              packet = build_packet(env, nil)
              yield packet, sp.bytesize
            rescue ex # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              raise ex
            end
            delete_message(sp)
          else
            begin
              id = next_id
              unless id
                @msg_store_lock.synchronize { @msg_store.requeue(sp) }
                return false
              end
              packet = build_packet(env, id)
              @unacked_count.add(1, :relaxed)
              @unacked_bytesize.add(sp.bytesize, :relaxed)
              yield packet, sp.bytesize
              @unacked[id] = sp
              @has_capacity.set(false) if @unacked.size >= Config.instance.max_inflight_messages
            rescue ex # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
              @unacked_count.sub(1, :relaxed)
              @unacked_bytesize.sub(sp.bytesize, :relaxed)
              raise ex
            end
          end
          return true
        end
        false
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise AMQP::Queue::ClosedError.new(cause: ex)
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

      private def apply_policy_argument(key : String, value : JSON::Any) : Bool
        @log.debug { "Applying policy #{key}: #{value}" }
        case key
        when "max-length"
          unless @max_length.try &.< value.as_i64
            @max_length = value.as_i64
            drop_overflow
            return true
          end
        when "max-length-bytes"
          unless @max_length_bytes.try &.< value.as_i64
            @max_length_bytes = value.as_i64
            drop_overflow
            return true
          end
        when "overflow"
          @reject_on_overflow ||= value.as_s == "reject-publish"
          return true
        end
        false
      end

      def after_policy_applied
        drop_overflow
      end

      def ack(packet : MQTT::PubAck) : Nil
        id = packet.packet_id
        if sp = @unacked.delete(id)
          begin
            @unacked.delete id
            @ack_count.add(1, :relaxed)
            @unacked_count.sub(1, :relaxed)
            @unacked_bytesize.sub(sp.bytesize, :relaxed)
            delete_message(sp)
          rescue ex
            raise ::IO::Error.new("Could not acknowledge packet with id '#{id}'", ex)
          end
          @has_capacity.set(true)
        else
          raise ::IO::Error.new("No message inflight for id '#{id}'")
        end
      end

      private def message_expire_loop; end

      private def queue_expire_loop; end

      private def next_id : UInt16?
        return if @unacked.size == Config.instance.max_inflight_messages
        start_id = @count
        next_id : UInt16 = start_id &+ 1_u16
        while @unacked.has_key?(next_id)
          next_id &+= 1u16
          next_id = 1u16 if next_id == 0
          return if next_id == start_id
        end
        @count = next_id
        next_id
      end

      private def delete_message(sp : SegmentPosition) : Nil
        @msg_store_lock.synchronize do
          @msg_store.delete(sp)
        end
      end

      private def drop_overflow : Nil
        return unless (ml = @max_length) || (mlb = @max_length_bytes)
        if ml = @max_length
          @msg_store_lock.synchronize do
            while @msg_store.size > ml
              env = @msg_store.shift? || break
              delete_message(env.segment_position)
            end
          end
        end
        if mlb = @max_length_bytes
          @msg_store_lock.synchronize do
            while @msg_store.bytesize > mlb
              env = @msg_store.shift? || break
              delete_message(env.segment_position)
            end
          end
        end
      end

      private def clear_policy_arguments
        handle_arguments
      end

      private def handle_arguments
        @effective_args = Array(String).new
        @effective_args << "x-queue-type"
      end

      def pause!; end

      def resume!; end

      def restart! : Bool
        false
      end

      def ack(sp : SegmentPosition) : Nil; end

      def reject(sp : SegmentPosition, requeue : Bool) : Nil; end

      def basic_get(no_ack : Bool, force : Bool = false, & : Envelope -> Nil) : Bool
        false
      end

      def state : QueueState
        @closed ? QueueState::Closed : QueueState::Running
      end

      def purge(max_count : Int = UInt32::MAX) : UInt32
        count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
        @log.info { "Purged #{count} messages" }
        count
      end

      def in_use? : Bool
        !(@msg_store.empty? && @client.nil?)
      end

      def match?(durable, exclusive, auto_delete, arguments) : Bool
        durable? == durable && @auto_delete == auto_delete
      end

      def unacked_messages
        Iterator(LavinMQ::UnackedMessage).empty
      end

      def to_json(json : JSON::Builder, consumer_limit : Int32 = -1)
        json.object do
          details_tuple.each do |k, v|
            json.field(k, v) unless v.nil?
          end
        end
      end

      def details_tuple
        stats = queue_stats_details
        {
          name:                         @name,
          durable:                      durable?,
          exclusive:                    false,
          auto_delete:                  @auto_delete,
          arguments:                    AMQP::Table.new,
          consumers:                    consumer_count,
          vhost:                        @vhost.name,
          messages:                     @msg_store.size + stats[:messages_unacknowledged],
          total_bytes:                  @msg_store.bytesize + stats[:message_bytes_unacknowledged],
          messages_persistent:          durable? ? @msg_store.size + stats[:messages_unacknowledged] : 0,
          ready:                        @msg_store.size,
          messages_ready:               @msg_store.size,
          ready_bytes:                  @msg_store.bytesize,
          message_bytes_ready:          @msg_store.bytesize,
          ready_avg_bytes:              @msg_store.avg_bytesize,
          unacked:                      stats[:unacked],
          messages_unacknowledged:      stats[:messages_unacknowledged],
          unacked_bytes:                stats[:unacked_bytes],
          message_bytes_unacknowledged: stats[:message_bytes_unacknowledged],
          unacked_avg_bytes:            stats[:unacked_avg_bytes],
          operator_policy:              operator_policy.try &.name,
          policy:                       policy.try &.name,
          exclusive_consumer_tag:       nil,
          single_active_consumer_tag:   nil,
          state:                        state,
          effective_policy_definition:  Policy.merge_definitions(policy, operator_policy),
          message_stats:                current_stats_details,
          effective_arguments:          @effective_args,
          effective_policy_arguments:   effective_policy_args,
          internal:                     false,
        }
      end
    end
  end
end
