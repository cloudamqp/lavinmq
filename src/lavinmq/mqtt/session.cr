require "digest/sha1"
require "./protocol"
require "../mqtt"
require "../amqp/queue/queue"
require "../error"
require "../sortable_json"
require "./client"
require "../policy"
require "../queue_stats"
require "../vhost"
require "./consts"
require "./session_message_store"

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      # A known packet id acknowledged with the wrong packet type. The client
      # must be disconnected [MQTT-4.8.0-1]; an unknown id is not this, because
      # the window does not survive a restart.
      class ProtocolViolation < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      Log = ::LavinMQ::Log.for "mqtt.session"

      ARGUMENTS      = AMQP::Table.new({"x-queue-type" => "mqtt"})
      EFFECTIVE_ARGS = {"x-queue-type"}

      # A packet id handed to the client and not yet settled. `sp` is nil only
      # for a QoS 2 id past PUBREC, where the message is gone and the id is held
      # for the PUBREL/PUBCOMP exchange alone.
      struct Inflight
        getter qos : UInt8
        getter sp : SegmentPosition?

        def initialize(@qos : UInt8, @sp : SegmentPosition?)
        end
      end

      getter name : String
      getter vhost : VHost
      getter? auto_delete

      @max_length : Int64? = nil
      @max_length_bytes : Int64? = nil
      @msg_store_lock = Mutex.new(:reentrant)
      @msg_store : SessionMessageStore
      @metadata : ::Log::Metadata
      @closed = Atomic(Bool).new(false)
      @deleted = false
      @client : MQTT::Client? = nil
      @has_client = BoolChannel.new(false)
      @has_capacity = BoolChannel.new(true)

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @unacked = Hash(UInt16, Inflight).new

        @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
        data_dir = File.join(
          durable? ? @vhost.data_dir : File.join(@vhost.data_dir, "transient"),
          Digest::SHA1.hexdigest(@name)
        )
        Dir.mkdir_p(data_dir) unless Dir.exists?(data_dir)
        replicator = durable? ? @vhost.@replicator : nil
        @msg_store = SessionMessageStore.new(data_dir, replicator, durable?, metadata: @metadata)

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
        ARGUMENTS
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

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        loop do
          break if closed?
          # Above every `next`: `loop` is inlined, so a raise from a guard below
          # would leave the rescue holding the previous iteration's connection.
          client = @client
          next @msg_store.empty.when_false.receive? if @msg_store.empty?
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
          # Sending yields, so a write can fail after the session was
          # reattached. Close the connection it was written to, not whichever
          # happens to be current.
          client.try &.close("Server force closed client")
          self.client = nil if @client == client
        end
      end

      # A resend keeps the packet id the client already knows [MQTT-4.4.0-1],
      # unless that id is still in flight - reissuing it would overwrite the
      # `@unacked` entry holding it - or is `0`, which may not go on the wire
      # [MQTT-2.3.1-5]. Both fall back to a fresh id.
      private def delivery_id(sp : SegmentPosition) : UInt16?
        if id = @msg_store.packet_id?(sp)
          return id unless id.zero? || @unacked.has_key?(id)
        end
        next_id
      end

      # `@has_capacity` mirrors "the in-flight window has room". Recomputed from
      # `@unacked` rather than written as a literal, since it is updated from both
      # the deliver_loop and the client's fiber and a stale `false` parks the
      # deliver_loop with no ack left to reopen the gate. `swap` rather than `set`
      # because this runs per delivery and per ack, and `set` takes both channel
      # locks even when the value is unchanged.
      private def refresh_capacity : Nil
        @has_capacity.swap(@unacked.size < Config.instance.max_inflight_messages)
      end

      # Whether `id` still names this exact delivery. Sending yields, so
      # `client=`, `ack` or `pubrec` can have moved it in the meantime.
      private def booked?(id : UInt16, sp : SegmentPosition) : Bool
        @unacked[id]?.try(&.sp) == sp
      end

      def client : MQTT::Client?
        @client
      end

      # `Client#close` usually joins the read fiber before `Broker#add_client`
      # reaches this, so an `ack`/`pubrec` is rarely in flight while `@unacked`
      # is walked - but a second `close` returns without waiting, so it can be.
      def client=(client : MQTT::Client?)
        return if closed?
        @last_get_time = RoughTime.instant

        # Ids past PUBREC, which owe a PUBREL rather than a message.
        pubcomp_pending = Array(UInt16).new

        # A clean session carries nothing between connections [MQTT-3.1.2-6]. A
        # persistent one requeues what it owes and remembers the packet ids, to
        # resend under the ids the client already knows [MQTT-4.4.0-1].
        unless clean_session?
          @msg_store_lock.synchronize do
            @unacked.each do |packet_id, inflight|
              if sp = inflight.sp
                @msg_store.remember_packet_id(sp, packet_id)
                @msg_store.requeue(sp)
              else
                pubcomp_pending << packet_id
              end
            end
          end
        end

        @unacked.clear
        @unacked_count.set(0, :release)
        @unacked_bytesize.set(0, :release)

        # Re-booked even when detaching: doing it only for an attached client
        # would drop the obligation on the disconnect it exists to survive.
        pubcomp_pending.each { |id| @unacked[id] = Inflight.new(2u8, nil) }
        refresh_capacity

        # Assigned before the writes below, which yield: `Session#publish`
        # drops a QoS 0 message while it is nil.
        @client = client
        if client
          @log.info { "resending #{pubcomp_pending.size} PUBREL" } unless pubcomp_pending.empty?
          # Before `@has_client` opens the gate, so these tend to precede the
          # replayed PUBLISHes. [MQTT-4.4.0-1] does not order the two kinds.
          pubcomp_pending.each { |id| send_pubrel(id, client) }
        end
        @has_client.set(!client.nil?)

        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable?
        !clean_session?
      end

      def subscribe(tf, qos)
        arguments = MQTT.qos_arguments(qos)
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
          # `nil` counts as QoS 0: `build_packet` maps it to 0, so booking an
          # id would leak the slot. Nothing produces a nil today.
          delivery_mode = env.message.properties.delivery_mode
          if delivery_mode.nil? || delivery_mode.zero?
            deliver_no_ack(env, sp) { |packet, bytesize| yield packet, bytesize }
          else
            delivered = deliver_acked(env, sp) { |packet, bytesize| yield packet, bytesize }
            return false unless delivered
          end
          return true
        end
        false
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ClosedError.new(cause: ex)
      end

      private def deliver_no_ack(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Nil
        begin
          yield build_packet(env, nil), sp.bytesize
          if env.redelivered
            @redeliver_count.add(1, :relaxed)
          else
            @deliver_no_ack_count.add(1, :relaxed)
            @deliver_get_count.add(1, :relaxed)
          end
        rescue ex # requeue failed delivery
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          raise ex
        end
        delete_message(sp)
      end

      # False when no packet id was available, which leaves the message
      # requeued for the next attempt.
      private def deliver_acked(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Bool
        id = delivery_id(sp)
        unless id
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          # Without this the deliver_loop spins: the store is non-empty and
          # capacity still reads true. Recomputed rather than closed
          # outright, since an ack can free a slot while the requeue above
          # waits on a contended @msg_store_lock.
          refresh_capacity
          return false
        end
        # Raises before anything is booked, which the rescue below would not
        # roll back. Unreachable today, but being wrong loses the message.
        packet = begin
          build_packet(env, id)
        rescue ex
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          raise ex
        end
        begin
          # Booked before the send, which yields: the client can acknowledge
          # before we return, and an acknowledgement finding no entry is either
          # fatal (`ack`) or silently dropped (`pubrec`).
          @unacked[id] = Inflight.new(packet.qos, sp)
          @unacked_count.add(1, :relaxed)
          @unacked_bytesize.add(sp.bytesize, :relaxed)
          yield packet, sp.bytesize
          if env.redelivered
            @redeliver_count.add(1, :relaxed)
          else
            @deliver_count.add(1, :relaxed)
            @deliver_get_count.add(1, :relaxed)
          end
          # `client=` may have requeued `sp` and remembered `id` during the
          # send; forgetting then costs the redelivery its id [MQTT-4.4.0-1].
          @msg_store.forget_packet_id(sp) if booked?(id, sp)
          refresh_capacity
        rescue ex # requeue failed delivery
          # Roll back only what is still ours: requeueing an entry `client=`
          # already requeued hands the message out twice.
          if booked?(id, sp)
            @unacked.delete(id)
            # Before the lock, which can park: `client=` zeroes both counters,
            # and a `sub` after that wraps an unsigned atomic.
            @unacked_count.sub(1, :relaxed)
            @unacked_bytesize.sub(sp.bytesize, :relaxed)
            @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          end
          raise ex
        end
        true
      end

      def build_packet(env, packet_id) : Protocol::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.["mqtt.retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        # `delivery_mode` is read off disk unvalidated and `Publish.new` raises
        # above QoS 2, which would make one bad byte a poison message.
        qos = 2u8 if qos > 2
        dup = qos.zero? ? false : env.redelivered
        Protocol::Publish.new(
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
        inflight = @unacked[id]?
        raise ::IO::Error.new("No message inflight for id '#{id}'") if inflight.nil?
        sp = inflight.sp
        # A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-2], so a PUBACK for
        # one is a protocol violation. Checked before the delete, so it cannot
        # drop an obligation the session still owes.
        if sp.nil? || inflight.qos != 1u8
          raise ProtocolViolation.new("PUBACK for packet id '#{id}', which is awaiting a QoS 2 acknowledgement")
        end
        @unacked.delete(id)
        begin
          @ack_count.add(1, :relaxed)
          @unacked_count.sub(1, :relaxed)
          @unacked_bytesize.sub(sp.bytesize, :relaxed)
          delete_message(sp)
        rescue ex
          raise ::IO::Error.new("Could not acknowledge packet with id '#{id}'", ex)
        ensure
          refresh_capacity
        end
      end

      # The receiver owns the message from PUBREC on [MQTT-4.3.3-1], so it is
      # deleted here, not at PUBCOMP; the id stays booked until then.
      #
      # Returns rather than raises for an unknown id: nothing in the window
      # survives a restart, so a client resuming across one always brings ids we
      # have never seen, and raising would publish its will.
      def pubrec(packet : Protocol::PubRec) : Bool
        id = packet.packet_id
        unless inflight = @unacked[id]?
          @log.warn { "PUBREC for unknown packet id '#{id}'" }
          return false
        end
        unless sp = inflight.sp
          # A repeat of a PUBREC we already answered, so our PUBREL was lost.
          # Answering again is the only way the client can release the id.
          send_pubrel(id)
          return false
        end
        unless inflight.qos == 2u8
          raise ProtocolViolation.new("PUBREC for QoS #{inflight.qos} packet id '#{id}'")
        end
        # Before the send: a failed write still leaves the correct state, and
        # `client=` re-sends the PUBREL.
        @unacked[id] = Inflight.new(2u8, nil)
        @ack_count.add(1, :relaxed)
        @unacked_count.sub(1, :relaxed)
        @unacked_bytesize.sub(sp.bytesize, :relaxed)
        delete_message(sp)
        send_pubrel(id)
        # No `refresh_capacity`: the id is still booked, so the window is
        # unchanged.
        true
      end

      def pubcomp(packet : Protocol::PubComp) : Bool
        id = packet.packet_id
        unless inflight = @unacked[id]?
          @log.warn { "PUBCOMP for unknown packet id '#{id}'" }
          return false
        end
        unless inflight.sp.nil?
          raise ProtocolViolation.new("PUBCOMP for packet id '#{id}' that has not been PUBRECed")
        end
        @unacked.delete(id)
        # Load-bearing: for a window full of ids awaiting PUBCOMP, this is the
        # only event that can reopen the capacity gate.
        refresh_capacity
        true
      end

      # Errors are swallowed: the id stays booked either way, so the next
      # attach re-sends it [MQTT-4.4.0-1].
      private def send_pubrel(id : UInt16, client : MQTT::Client? = nil) : Bool
        client ||= @client
        return false if client.nil?
        client.send(Protocol::PubRel.new(id))
        true
      rescue ex
        @log.debug { "Failed to send PUBREL for id '#{id}': #{ex.message}" }
        false
      end

      private def next_id : UInt16?
        # `>=` not `==`: the limit is mutable at runtime, so the window can
        # already be over it.
        return if @unacked.size >= Config.instance.max_inflight_messages
        start_id = @count
        next_id : UInt16 = start_id &+ 1_u16
        # `@count` at 65535 wraps this to 0, which the loop below never
        # corrects because 0 is never booked. Packet id 0 is illegal
        # [MQTT-2.3.1-1].
        next_id = 1u16 if next_id == 0
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
        durable? == durable && @auto_delete == auto_delete
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
          auto_delete:                  @auto_delete,
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
