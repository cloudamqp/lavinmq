require "digest/sha1"
require "./protocol"
require "./publish_headers"
require "../mqtt"
require "../amqp/queue/queue"
require "../error"
require "../sortable_json"
require "./client"
require "../policy"
require "../queue_stats"
require "../vhost"
require "./consts"

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      Log = ::LavinMQ::Log.for "mqtt.session"

      ARGUMENTS      = AMQP::Table.new({"x-queue-type" => "mqtt"})
      EFFECTIVE_ARGS = {"x-queue-type"}

      # Per-instance, not the ARGUMENTS constant: definitions_store persists a
      # session as a Queue::Declare frame carrying this table, so it is the only
      # place session state can survive a restart. Never mutated, so sharing the
      # constant as the default is safe - but it MUST keep x-queue-type, or
      # QueueFactory rebuilds the session as a plain AMQP queue on the next boot.
      @arguments : AMQP::Table

      getter name : String
      getter vhost : VHost

      # Seconds the session outlives its connection [MQTT-3.1.2.11.2]. 0 means it
      # ends when the connection closes, UInt32::MAX means it never expires.
      # Single source for auto_delete? and the expiry clock.
      getter session_expiry_interval : UInt32

      # Derived from the interval once, at construction: it selects the data dir,
      # the replicator and the message store's durability, none of which can be
      # re-derived once that store exists.
      @durable : Bool

      @max_length : Int64? = nil
      @max_length_bytes : Int64? = nil
      @msg_store_lock = Mutex.new(:reentrant)
      @msg_store : MessageStore
      @metadata : ::Log::Metadata
      @closed = Atomic(Bool).new(false)
      @deleted = false
      @client : MQTT::Client? = nil
      @has_client = BoolChannel.new(false)
      @has_capacity = BoolChannel.new(true)

      protected def initialize(@vhost : VHost,
                               @name : String,
                               auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = ARGUMENTS)
        @arguments = arguments
        @session_expiry_interval = self.class.expiry_from(@name, arguments, auto_delete)
        @durable = !@session_expiry_interval.zero?
        @count = 0u16
        @unacked = Hash(UInt16, SegmentPosition).new

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

      def closed?
        @closed.get(:acquire)
      end

      def consumer_count : UInt32
        @client.nil? ? 0u32 : 1u32
      end

      def message_count : UInt32
        @msg_store.size.to_u32
      end

      def exclusive? : Bool
        false
      end

      def arguments : AMQP::Table
        @arguments
      end

      # The argument wins when usable. Bounds-checked rather than cast, and warned
      # about rather than swallowed, because an AMQP client can declare mqtt.<id>
      # by hand with any int type in there - or something that is not an int.
      #
      # Without a usable one, fall back to what the declare flag meant before
      # Session Expiry Interval existed - auto_delete was clean_session, so a
      # durable session meant "keep forever". That covers both a definitions file
      # written by an older LavinMQ and a caller declaring a session directly.
      protected def self.expiry_from(name : String,
                                     arguments : ::AMQ::Protocol::Table,
                                     auto_delete : Bool) : UInt32
        v = arguments[SESSION_EXPIRY_ARG]?
        if v.is_a?(Int)
          return v.to_u32 if v >= 0 && v <= UInt32::MAX
          Log.warn { "#{name}: ignoring out-of-range #{SESSION_EXPIRY_ARG}=#{v}" }
        elsif !v.nil?
          Log.warn { "#{name}: ignoring non-integer #{SESSION_EXPIRY_ARG}=#{v.inspect}" }
        end
        auto_delete ? 0u32 : UInt32::MAX
      end

      def close : Bool
        return false if @closed.swap(true)
        @has_capacity.close
        @has_client.close
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

      def auto_delete? : Bool
        @session_expiry_interval.zero?
      end

      # A reconnecting client may name a different interval, and so may its
      # DISCONNECT [MQTT-3.14.2.2.2]. @arguments carries it, but the definitions
      # log has no update frame for an existing queue, so it only reaches disk at
      # the next compaction.
      #
      # auto_delete? and the expiry clock follow this; durable? deliberately does
      # not, so narrowing to 0 still writes a persisted deletion frame rather than
      # leaving the original declare to replay.
      def session_expiry_interval=(interval : UInt32) : Nil
        return if interval == @session_expiry_interval
        @session_expiry_interval = interval
        # clone, never mutate: @arguments may be the shared ARGUMENTS constant.
        args = @arguments.clone
        args[SESSION_EXPIRY_ARG] = interval
        @arguments = args
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        loop do
          break if closed?
          # Client before store: an offline session has to park on @has_client,
          # both so the expiry clock runs and so it does not wake on a publish it
          # cannot deliver.
          client = @client
          next wait_for_client if client.nil?
          next wait_for_messages if @msg_store.empty?
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

      # Parks until there is something to deliver. The detach arm matters: on its
      # own, a park on @msg_store.empty never wakes when the client leaves, so the
      # loop would never reach the top again to start the expiry clock.
      private def wait_for_messages : Nil
        select
        when @msg_store.empty.when_false.receive?
        when @has_client.when_false.receive?
        end
      end

      # Parks until a client attaches, or until the session expires. This is the
      # only place the expiry clock runs - exactly the window in which the session
      # has no connection [MQTT-3.1.2.11.2]. Reattaching cancels the timer, and
      # the next disconnect enters a fresh select, so the interval is measured
      # from each disconnect rather than accumulated.
      private def wait_for_client : Nil
        ttl = @session_expiry_interval
        # Unreachable in practice - Broker#remove_client deletes a 0-interval
        # session - but expiring is the right answer if it is ever reached.
        return expire if ttl.zero?
        if ttl == UInt32::MAX
          @has_client.when_true.receive?
          return
        end
        select
        when @has_client.when_true.receive?
        when timeout ttl.seconds
          expire
        end
      end

      # Runs on the session's own fiber. `delete` closes @has_client and the
      # message store, so deliver_loop's `break if closed?` exits on the next
      # pass; the re-entrant q.delete from @vhost.delete_queue is a no-op via
      # @deleted.
      private def expire : Nil
        @log.info { "Session expired after #{@session_expiry_interval}s offline" }
        delete
      end

      def client : MQTT::Client?
        @client
      end

      def client=(client : MQTT::Client?)
        return if closed?
        @last_get_time = RoughTime.instant

        if durable?
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

      def durable? : Bool
        @durable
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

      # Returns whether a matching subscription existed, so the v5 UNSUBACK can
      # report Success vs NoSubscriptionExisted per topic filter [MQTT-3.11.3].
      def unsubscribe(tf) : Bool
        if binding = find_binding(tf)
          unbind(tf, binding.binding_key.arguments)
          true
        else
          false
        end
      end

      def publish(msg : Message) : Bool
        return true if msg.properties.delivery_mode == 0 && @client.nil?
        return false if @deleted || closed?
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

      private def get_packet(& : Protocol::Publish, UInt32 -> Nil) : Bool
        raise ClosedError.new if closed?
        loop do
          env = @msg_store_lock.synchronize { @msg_store.shift? } || break
          sp = env.segment_position
          no_ack = env.message.properties.delivery_mode == 0
          if no_ack
            begin
              packet = build_packet(env, nil)
              if exceeds_max_packet_size?(packet)
                delete_message(sp)
                next
              end
              yield packet, sp.bytesize
              record_delivery(env.redelivered, no_ack)
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
              if exceeds_max_packet_size?(packet)
                # Discard without sending and complete the delivery: do not track
                # it in @unacked, so it is not redelivered [MQTT-3.1.2-25].
                delete_message(sp)
                next
              end
              @unacked_count.add(1, :relaxed)
              @unacked_bytesize.add(sp.bytesize, :relaxed)
              begin
                yield packet, sp.bytesize
                record_delivery(env.redelivered, no_ack)
                @unacked[id] = sp
                @has_capacity.set(false) if @unacked.size >= Config.instance.max_inflight_messages
              rescue ex # roll back only what this block added
                # Scoped tightly on purpose: build_packet above can raise, and
                # subtracting a count that was never added goes negative.
                @unacked_count.sub(1, :relaxed)
                @unacked_bytesize.sub(sp.bytesize, :relaxed)
                raise ex
              end
            rescue ex # requeue failed delivery
              @msg_store_lock.synchronize { @msg_store.requeue(sp) }
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

      private def record_delivery(redelivered : Bool, no_ack : Bool) : Nil
        if redelivered
          @redeliver_count.add(1, :relaxed)
        else
          (no_ack ? @deliver_no_ack_count : @deliver_count).add(1, :relaxed)
          @deliver_get_count.add(1, :relaxed)
        end
      end

      # A v5 client's Maximum Packet Size caps the packets we may send it
      # [MQTT-3.1.2-24]. Only v5 clients set it, so size against v5 framing.
      private def exceeds_max_packet_size?(packet : Protocol::Publish) : Bool
        max = @client.try(&.max_packet_size) || return false
        return false unless packet.bytesize(Protocol::Version::V5) > max
        @log.debug { "Dropping PUBLISH exceeding client Maximum Packet Size (#{max} bytes)" }
        true
      end

      def build_packet(env, packet_id) : Protocol::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.["mqtt.retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        qos = MAX_QOS if qos > MAX_QOS
        dup = qos.zero? ? false : env.redelivered
        # IO::V3#write_properties discards these, so a v3 subscriber should not
        # pay six Table#fetch linear scans per delivery to build them.
        properties = if @client.try(&.version.v5?)
                       PublishHeaders.restore(msg.properties.headers)
                     else
                       Protocol::PublishProperties.new
                     end
        Protocol::Publish.new(
          packet_id: packet_id,
          payload: msg.body,
          dup: dup,
          qos: qos,
          retain: retained,
          topic: msg.routing_key,
          properties: properties
        )
      end

      private def apply_policy_argument(key : String, value : JSON::Any) : Bool
        @log.debug { "Applying policy #{key}: #{value}" }
        case key
        when "max-length"
          if @max_length.nil?
            @max_length = value.as_i64
            return true
          end
        when "max-length-bytes"
          if @max_length_bytes.nil?
            @max_length_bytes = value.as_i64
            return true
          end
        end
        false
      end

      def after_policy_applied
        drop_overflow
      end

      def ack(packet : Protocol::PubAck) : Nil
        id = packet.packet_id
        if sp = @unacked.delete(id)
          begin
            @ack_count.add(1, :relaxed)
            @unacked_count.sub(1, :relaxed)
            @unacked_bytesize.sub(sp.bytesize, :relaxed)
            delete_message(sp)
          rescue ex
            raise ::IO::Error.new("Could not acknowledge packet with id '#{id}'", ex)
          ensure
            @has_capacity.set(true)
          end
        else
          raise ::IO::Error.new("No message inflight for id '#{id}'")
        end
      end

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
        @max_length = nil
        @max_length_bytes = nil
      end

      private def handle_arguments
      end

      def pause!; end

      def resume!; end

      def restart! : Bool
        false
      end

      def state : QueueState
        closed? ? QueueState::Closed : QueueState::Running
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
        durable? == durable && auto_delete? == auto_delete
      end

      def unacked_messages
        Array(LavinMQ::UnackedMessage).new
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
          auto_delete:                  auto_delete?,
          arguments:                    NamedTuple.new, # "empty" AMQP::Table
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
          effective_arguments:          EFFECTIVE_ARGS,
          effective_policy_arguments:   effective_policy_args,
          internal:                     false,
        }
      end
    end
  end
end
