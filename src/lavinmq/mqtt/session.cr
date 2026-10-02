require "../filesystem"
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
require "./permission_service"
require "./session_message_store"

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      Log = ::LavinMQ::Log.for "mqtt.session"

      ARGUMENTS = AMQP::Table.new({"x-queue-type" => "mqtt"})
      # The arguments a session actually acts on, reported as `effective_arguments`
      # over the HTTP API. Both are honoured whether declared by a client or read
      # back from the definitions file.
      EFFECTIVE_ARGS = {"x-queue-type", SESSION_EXPIRY_ARG}

      # Per-instance, not the ARGUMENTS constant: definitions_store persists a
      # session as a Queue::Declare frame carrying this table, so it is the only
      # place session state can survive a restart. Never mutated, so sharing the
      # constant as the default is safe - but it MUST keep x-queue-type, or
      # QueueFactory rebuilds the session as a plain AMQP queue on the next boot.
      @arguments : AMQP::Table

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
      getter? internal = false
      getter? deleted = false

      # Seconds the session outlives its connection (§3.1.2.11.2). 0 means it
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
      @msg_store : SessionMessageStore
      @metadata : ::Log::Metadata
      @closed = Atomic(Bool).new(false)
      @client : MQTT::Client? = nil
      @permission_service : PermissionService
      # Derived from the queue name, so a restored session with no client
      # attached still knows its client id.
      @client_id : String
      # Carries the user of the last attached client. The username is kept in
      # the .metadata file so a restored session keeps its member rules until a
      # client reconnects.
      @permission_context : PermissionService::Context
      @metadata_file : String
      @replicator : Clustering::Replicator?
      @has_client = BoolChannel.new(false)
      @has_capacity = BoolChannel.new(true)
      # Packet ids of QoS 2 PUBLISHes answered with PUBREC and not yet released.
      # Holding the id is the whole of the guarantee: a re-sent PUBLISH carrying
      # one is answered again and not routed twice [MQTT-4.3.3-10].
      @qos2_received = Set(UInt16).new

      protected def initialize(@vhost : VHost,
                               @name : String,
                               auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = ARGUMENTS)
        @arguments = arguments
        @session_expiry_interval = self.class.expiry_from(@name, arguments, auto_delete)
        @durable = !@session_expiry_interval.zero?
        @count = 0u16
        @client_id = @name.lchop(SESSION_PREFIX)
        @permission_service = @vhost.mqtt_permission_service
        @unacked = Hash(UInt16, Inflight).new

        @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
        data_dir = File.join(
          durable? ? @vhost.data_dir : File.join(@vhost.data_dir, "transient"),
          Digest::SHA1.hexdigest(@name)
        )
        FileSystem.mkdir_p(data_dir)
        @replicator = durable? ? @vhost.@replicator : nil
        @msg_store = SessionMessageStore.new(data_dir, @replicator, durable?, metadata: @metadata, persister: @vhost.persister)
        @metadata_file = File.join(data_dir, ".metadata")
        username = nil
        if File.exists?(@metadata_file)
          @replicator.try &.register_file(@metadata_file)
          username = read_metadata_file
        end
        @permission_context = PermissionService::Context.new(username, @client_id)

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
        # Every connection has a session, so one left without it has nowhere to
        # hold its state. Only the socket is closed: this can run under the
        # definitions lock, which the read fiber may be waiting on, and leaving
        # `@closed` unset keeps a takeover waiting for that fiber to exit.
        if client = @client
          @log.info { "Session deleted, disconnecting client '#{client.name}'" }
          client.force_close
        end
        close
        @msg_store_lock.synchronize do
          @msg_store.delete
        end
        @replicator.try &.delete_file(@metadata_file)
        @vhost.delete_queue(@name)
        true
      end

      def auto_delete? : Bool
        @session_expiry_interval.zero?
      end

      # A reconnecting client may name a different interval, and so may its
      # DISCONNECT (§3.14.2.2.2). @arguments carries it, but the definitions
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
          # Above every `next`: `loop` is inlined, so a raise from a guard below
          # would leave the rescue holding the previous iteration's connection.
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
      # [MQTT-2.2.1-4]. Both fall back to a fresh id.
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
      # has no connection (§3.1.2.11.2). Reattaching cancels the timer, and
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

      # A takeover's `Client#close` usually joins the old read fiber before the
      # new `Client#run` reaches this, so an `ack`/`pubrec` is rarely in flight while `@unacked`
      # is walked - but a second `close` returns without waiting, so it can be.
      def client=(client : MQTT::Client?)
        # A closed store can't be touched, but `delete` still has to know which
        # connection to close.
        return @client = client if closed?
        @last_get_time = RoughTime.instant

        # Ids past PUBREC, which owe a PUBREL rather than a message.
        pubcomp_pending = Array(UInt16).new

        # Only a session with a non-zero expiry outlives its connection
        # [MQTT-3.1.2-23]. It requeues what it owes and remembers the packet ids, to
        # resend under the ids the client already knows [MQTT-4.4.0-1].
        if durable?
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
        if client && (username = client.user.name) != @permission_context.username
          @permission_context = PermissionService::Context.new(username, @client_id)
          write_metadata_file(username) if durable?
        end

        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable? : Bool
        @durable
      end

      # The .metadata file holds the last attached username, so a restored
      # session keeps its member rules. It only exists for a durable session a
      # client has attached to; nothing else has a username to restore. Anything that is not a JSON object with a string username is treated
      # as an unknown user; a bad file must never stop the session from loading.
      private def read_metadata_file : String?
        JSON.parse(File.read(@metadata_file)).as_h?.try(&.["username"]?).try(&.as_s?)
      rescue ex : JSON::ParseException | IO::Error
        @log.warn(exception: ex) { "Could not read #{@metadata_file}, session user unknown until a client connects" }
        nil
      end

      # Written to a temporary file and renamed into place, so a crash
      # mid-write leaves the previous file rather than a truncated one. Only for
      # a durable session: a lost username refuses the offline messages its
      # member rules allow.
      private def write_metadata_file(username : String) : Nil
        FileSystem.replace(@metadata_file) do |f|
          {name: @name, client_id: @client_id, username: username}.to_json(f)
        end
        @replicator.try &.replace_file(@metadata_file)
      end

      # Returns whether this filter had no subscription before, so Retain
      # Handling 1 replays the retain store only for a genuinely new
      # subscription. Existence is by topic filter alone: [MQTT-3.8.4-3]
      # replaces a subscription whose filter is identical, so a re-subscribe
      # that changes the QoS or the options is a replacement, not a new one.
      def subscribe(tf, options : SubscriptionOptions) : Bool
        existing = find_binding(tf)
        if existing
          # Compare the options, not the rendered tables: two tables per
          # re-subscribe is pure waste, and this does not lean on Table#==.
          return false if existing.binding_key.options == options
          unbind(tf, existing.binding_key.arguments)
        end
        @vhost.bind_queue(@name, EXCHANGE, tf, MQTT.subscription_arguments(options))
        existing.nil?
      end

      # Returns whether a matching subscription existed, so the v5 UNSUBACK can
      # report Success vs NoSubscriptionExisted per topic filter [MQTT-3.11.3-2].
      def unsubscribe(tf) : Bool
        if binding = find_binding(tf)
          unbind(tf, binding.binding_key.arguments)
          true
        else
          false
        end
      end

      # Returns whether the message was accepted, so the exchange only counts
      # deliveries that happened.
      def publish(msg : Message) : Bool
        unless @permission_service.can_read?(@permission_context, msg.routing_key)
          @log.debug { "Message refused: no topic permission rule allows user '#{@permission_context.username}' to read topic '#{msg.routing_key}'" }
          return false
        end
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

      # The type check is load-bearing: `queue_bindings` prepends a synthetic
      # default-exchange binding whose routing key is the queue's own name, so
      # matching on routing key alone makes a subscription to the literal filter
      # `mqtt.<own client id>` find that instead of its own binding. Only
      # `MQTT::Exchange` produces `SubscriptionDetails`, and there is one of
      # those, so this is exact - and it narrows the union enough for `options`.
      private def find_binding(rk) : SubscriptionDetails?
        bindings.each do |b|
          next unless b.is_a?(SubscriptionDetails)
          return b if b.binding_key.routing_key == rk
        end
        nil
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
          result = if delivery_mode.nil? || delivery_mode.zero?
                     deliver_no_ack(env, sp) { |packet, bytesize| yield packet, bytesize }
                   else
                     deliver_acked(env, sp) { |packet, bytesize| yield packet, bytesize }
                   end
          case result
          in .sent?         then return true
          in .discarded?    then next
          in .no_packet_id? then return false
          end
        end
        false
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ClosedError.new(cause: ex)
      end

      private enum Delivery
        Sent
        # Over the client's Maximum Packet Size, and dropped unsent.
        Discarded
        NoPacketId
      end

      private def deliver_no_ack(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Delivery
        begin
          packet = build_packet(env, nil)
          if exceeds_max_packet_size?(packet)
            delete_message(sp)
            return Delivery::Discarded
          end
          yield packet, sp.bytesize
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
        Delivery::Sent
      end

      # `NoPacketId` leaves the message requeued for the next attempt.
      private def deliver_acked(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Delivery
        id = delivery_id(sp)
        unless id
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          # Without this the deliver_loop spins: the store is non-empty and
          # capacity still reads true. Recomputed rather than closed
          # outright, since an ack can free a slot while the requeue above
          # waits on a contended @msg_store_lock.
          refresh_capacity
          return Delivery::NoPacketId
        end
        # Raises before anything is booked, which the rescue below would not
        # roll back. Unreachable today, but being wrong loses the message.
        packet = begin
          build_packet(env, id)
        rescue ex
          @msg_store_lock.synchronize { @msg_store.requeue(sp) }
          raise ex
        end
        if exceeds_max_packet_size?(packet)
          # Discard without sending and complete the delivery: the id is never
          # booked, so it is not redelivered [MQTT-3.1.2-25].
          delete_message(sp)
          return Delivery::Discarded
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
        Delivery::Sent
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
        retained = msg.properties.try &.headers.try &.[RETAIN_HEADER]? == true
        # `delivery_mode` is read off disk unvalidated and `Publish.new` raises
        # above QoS 2, which would make one bad byte a poison message.
        qos = MQTT.granted_qos(msg.properties.delivery_mode)
        dup = qos.zero? ? false : env.redelivered
        # IO::Framing::V3#write_properties discards these, so a v3 subscriber
        # should not pay six Table#fetch linear scans per delivery to build them.
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
        inflight = @unacked[id]?
        raise ::IO::Error.new("No message inflight for id '#{id}'") if inflight.nil?
        sp = inflight.sp
        # A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-3], so a PUBACK for
        # one is a protocol violation. Checked before the delete, so it cannot
        # drop an obligation the session still owes.
        if sp.nil? || inflight.qos != 1u8
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBACK for packet id '#{id}', which is awaiting a QoS 2 acknowledgement")
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

      # The receiver owns the message from PUBREC on [MQTT-4.3.3-8], so it is
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
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBREC for QoS #{inflight.qos} packet id '#{id}'")
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
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBCOMP for packet id '#{id}' that has not been PUBRECed")
        end
        @unacked.delete(id)
        # Load-bearing: for a window full of ids awaiting PUBCOMP, this is the
        # only event that can reopen the capacity gate.
        refresh_capacity
        true
      end

      # Records `packet_id`, returning false if it was already held, i.e. this
      # PUBLISH is a re-send of one already routed.
      #
      # Uncapped on purpose: ids are `UInt16` so a session holds at most 65535,
      # and rejecting past a cap would have to raise, which publishes the will.
      def qos2_publish_received?(packet_id : UInt16) : Bool
        @qos2_received.add?(packet_id)
      end

      # Releases `packet_id` on PUBREL. False if we were not holding it.
      def qos2_release(packet_id : UInt16) : Bool
        @qos2_received.delete(packet_id)
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
        # [MQTT-2.2.1-4].
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

      def state_match?(states : Array(QueueState)) : Bool
        states.includes?(state)
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
