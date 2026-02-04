require "../amqp/queue/queue"
require "../error"
require "../queue_stats"
require "./consts"

module LavinMQ
  module MQTT
    class ClosedError < Error; end

    class Session < LavinMQ::Queue
      include PolicyTarget
      include QueueStats
      include SortableJSON

      ARGUMENTS = AMQP::Table.new({"x-queue-type": "mqtt"})
      Log       = ::LavinMQ::Log.for "mqtt.session"

      getter name
      getter arguments : AMQP::Table = ARGUMENTS
      getter? clean_session
      getter? closed = false
      getter? deleted = false
      getter state = QueueState::Running

      @msg_store : MessageStore
      @msg_store_lock = Mutex.new(:reentrant)
      @has_client = BoolChannel.new(false)

      @client : MQTT::Client? = nil

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @clean_session = false)
        @count = 0u16
        @unacked = Hash(UInt16, SegmentPosition).new
        @metadata = ::Log::Metadata.new(nil, {session: @name, vhost: @vhost.name})
        data_dir = make_data_dir
        @msg_store = init_msg_store(data_dir)
        @log = Logger.new(Log, @metadata)
        spawn deliver_loop, name: "Session#deliver_loop"
      end

      private def init_msg_store(data_dir)
        replicator = durable? ? @vhost.@replicator : nil
        MessageStore.new(data_dir, replicator, durable?, metadata: @metadata)
      end

      private def make_data_dir : String
        data_dir = if durable?
                     File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
                   else
                     File.join(@vhost.data_dir, "transient", Digest::SHA1.hexdigest @name)
                   end
        if Dir.exists? data_dir
          # delete left over files from transient queues
          unless durable?
            FileUtils.rm_r data_dir
            Dir.mkdir_p data_dir
          end
        else
          Dir.mkdir_p data_dir
        end
        data_dir
      end

      def clear_policy_arguments
      end

      # Return true if applied
      def apply_policy_argument(key : String, value : JSON::Any) : Bool
        false
      end

      def empty? : Bool
        @msg_store.empty?
      end

      private def deliver_loop
        i = 0
        loop do
          break if closed?
          next @msg_store.empty.when_false.receive? if @msg_store.empty?
          next @has_client.when_true.receive? unless @has_client.value
          if client = @client
            get_packet do |pub_packet|
              client.send(pub_packet)
            end
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        rescue ex
          @log.error(exception: ex) { "Failed to deliver message in deliver_loop" }
          @client.try &.close
          self.client = nil
        end
      end

      private def add_consumer(consumer : Client::Channel::Consumer)
      end

      private def rm_consumer(consumer : Client::Channel::Consumer)
      end

      def details_tuple
        stats = queue_stats_details
        {
          name:        @name,
          durable:     durable?,
          exclusive:   false,
          auto_delete: clean_session?,
          # arguments:                    {},
          consumers:                    @client.nil? ? 0 : 1,
          vhost:                        @vhost.name,
          messages:                     @msg_store.size + stats[:messages_unacknowledged],
          total_bytes:                  @msg_store.bytesize + stats[:message_bytes_unacknowledged],
          messages_persistent:          durable? ? @msg_store.size + stats[:messages_unacknowledged] : 0,
          ready:                        @msg_store.size, # Deprecated, to be removed in next major version
          messages_ready:               @msg_store.size,
          ready_bytes:                  @msg_store.bytesize, # Deprecated, to be removed in next major version
          message_bytes_ready:          @msg_store.bytesize,
          ready_avg_bytes:              @msg_store.avg_bytesize,
          unacked:                      stats[:unacked], # Deprecated, to be removed in next major version
          messages_unacknowledged:      stats[:messages_unacknowledged],
          unacked_bytes:                stats[:unacked_bytes], # Deprecated, to be removed in next major version
          message_bytes_unacknowledged: stats[:message_bytes_unacknowledged],
          unacked_avg_bytes:            stats[:unacked_avg_bytes],
          operator_policy:              @operator_policy.try &.name,
          policy:                       @policy.try &.name,
          exclusive_consumer_tag:       nil,
          single_active_consumer_tag:   nil,
          state:                        @state,
          effective_policy_definition:  nil, # Policy.merge_definitions(@policy, @operator_policy),
          message_stats:                current_stats_details,
          effective_arguments:          nil,
          effective_policy_arguments:   nil,
          internal:                     false,
        }
      end

      def client : MQTT::Client?
        @client
      end

      def client=(client : MQTT::Client?)
        return if closed?
        # We're done if we hold the same client already
        return if !client.nil? && (@client == client)
        @last_get_time = RoughTime.instant

        unless clean_session?
          @msg_store_lock.synchronize do
            @unacked.values.each do |sp|
              @msg_store.requeue(sp)
            end
          end
        end
        @unacked.clear

        @log.debug { "client set to '#{client.try &.name}'" }

        if @client = client
          @has_client.set(true)
        else
          @has_client.set(false)
        end
      end

      def durable?
        !clean_session?
      end

      def auto_delete?
        clean_session?
      end

      def match?(frame)
        durable? == frame.durable &&
          false == frame.exclusive &&
          clean_session? == frame.auto_delete &&
          frame.arguments == arguments
      end

      def match?(durable, exclusive, auto_delete, arguments)
        durable? == durable &&
          false == exclusive &&
          clean_session? == auto_delete &&
          arguments == arguments
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
        return false if @deleted || @state.closed?
        # Do not enqueue messages with QoS 0 if there are no consumers subscribed to the session
        return true if msg.properties.delivery_mode == 0 && @client.nil?
        @msg_store_lock.synchronize do
          @msg_store.push(msg)
        end
        @publish_count.add(1, :relaxed)
        true
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ex
      end

      def delete : Bool
        return false if @deleted
        @deleted = true
        close
        @state = QueueState::Deleted
        @msg_store_lock.synchronize do
          @msg_store.delete
        end
        @vhost.delete_queue(@name)
        @log.info { "(messages=#{message_count}) Deleted" }
        true
      end

      private def find_binding(rk)
        bindings.find { |b| b.binding_key.routing_key == rk }
      end

      def bindings
        @vhost.queue_bindings(self)
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, EXCHANGE, rk, arguments || AMQP::Table.new)
      end

      private def get_packet(& : MQTT::Publish -> Nil) : Bool
        raise ClosedError.new if closed?
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
            begin
              id = next_id
              return false unless id
              packet = build_packet(env, id)
              @unacked_count.add(1, :relaxed)
              @unacked_bytesize.add(sp.bytesize, :relaxed)
              yield packet
              @unacked[id] = sp
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
        raise ClosedError.new(cause: ex)
      end

      def close : Bool
        return false if @closed
        @closed = true
        @state = QueueState::Closed
        @client.try &.close("session closed")
        @msg_store_lock.synchronize do
          @msg_store.close
        end
        Fiber.yield
        @log.debug { "Closed" }
        true
      end

      def message_count
        @msg_store.size.to_u32
      end

      protected def delete_message(sp : SegmentPosition) : Nil
        {% unless flag?(:release) %}
          @log.debug { "Deleting: #{sp}" }
        {% end %}
        @msg_store_lock.synchronize do
          @msg_store.delete(sp)
        end
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

      def ack(packet : MQTT::PubAck) : Nil
        return if @deleted
        id = packet.packet_id
        sp = @unacked[id]
        @unacked.delete id
        @log.debug { "Acking #{sp}" }
        @ack_count.add(1, :relaxed)
        @unacked_count.sub(1, :relaxed)
        @unacked_bytesize.sub(sp.bytesize, :relaxed)
        delete_message(sp)
      rescue
        raise ::IO::Error.new("Could not acknowledge package with id: #{id}")
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

      private def handle_arguments
        super
        @effective_args << "x-queue-type"
      end

      def purge(max_count : Int = UInt32::MAX) : UInt32
        if unacked_count == 0 && max_count >= message_count
          # If there's no unacked and we're purging all messages, we can purge faster by deleting files
          delete_count = message_count
          @msg_store_lock.synchronize { @msg_store.purge_all }
        else
          delete_count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
        end
        @log.info { "Purged #{delete_count} messages" }
        delete_count
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ex
      end

      def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
        raise "Not supported"
      end

      def ack(sp : SegmentPosition) : Nil
        raise "Not supported"
      end

      def reject(sp : SegmentPosition, requeue : Bool)
        raise "Not supported"
      end

      def pause!
        raise "Not supported"
      end

      def resume!
        raise "Not supported"
      end

      def restart!
        raise "Not supported"
      end
    end
  end
end
