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
require "./subscriber"
require "./packet_id_log"
require "../persister"
require "./pending_will"

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      # 3.1.1 has no way to refuse one publish, so going over the cap closes
      # the connection; the client re-sends its PUBRELs on reconnect.
      class AwaitingPubrelLimitReached < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      include MQTT::Subscriber
      include Persister::ConfirmTarget
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

      # A packet id handed to the client and not completely acknowledged
      # [MQTT-4.3.2-3] [MQTT-4.3.3-3]. `sp` is nil only when awaiting PUBCOMP:
      # the message is gone and the id is held for the PUBREL/PUBCOMP exchange.
      struct Inflight
        enum Awaiting : UInt8
          PubAck
          PubRec
          PubComp
        end

        getter awaiting : Awaiting
        getter sp : SegmentPosition?

        def initialize(@awaiting : Awaiting, @sp : SegmentPosition?)
        end

        def qos : UInt8
          awaiting.pub_ack? ? 1u8 : 2u8
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
      # Created by the first subscription, see #msg_store
      @msg_store : SessionMessageStore? = nil
      @data_dir : String
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
      # Set by `arm_will`, published by this session's fiber (§3.1.3.2.2).
      getter pending_will : PendingWill? = nil
      # A connection for this client id is registered but not attached yet
      # (`Broker#add_client_locked`, then `Client#run`). Expiry waits for it:
      # a session is only deleted under the client-id lock (rule 1 in broker.cr).
      @resuming = BoolChannel.new(false)
      # The expiry clock of the current offline window, fixed when it starts:
      # neither a will firing mid-wait nor a resuming connection narrowing the
      # interval before it attaches may move it.
      @offline_since = Time.instant
      @offline_ttl : UInt32
      # Packet ids of QoS 2 PUBLISHes answered with PUBREC and not yet released.
      # Holding the id is the whole of the guarantee: a re-sent PUBLISH carrying
      # one is answered again and not routed twice [MQTT-4.3.3-10]. Not derived
      # from the log's set: a clean session has no log.
      @awaiting_pubrel = Set(UInt16).new
      # Ids PUBREL'd for an unknown PUBREC, held until PUBCOMP so they are not
      # reused. Outside `@inflight`: they are not our deliveries, and a client
      # that never answers must not take up the window.
      @unknown_pubrecs = Set(UInt16).new
      # Durable sessions only: a clean session's state ends with its
      # connection [MQTT-3.1.2-6].
      @packet_id_log : PacketIdLog?
      # Durable sessions only: holds a QoS 2 PUBLISH until its packet id is
      # durable, since a subscriber holding an id we forgot dedupes the next
      # message we send under it [MQTT-4.3.3-2]. Only the deliver_loop waits.
      @durable_mailbox : ::Channel(UInt64)?
      @durable_seq = 0u64
      # Reused by `deliver_qos2_batch`, so a batch allocates nothing once grown
      @qos2_batch = Array({Envelope, UInt16, Bool}).new
      # Routed inbound ids whose PUBLISH_RECEIVED record waits for a drain, by
      # routing generation: a barrier left by an older connection must not
      # record an id the client released and reused since.
      @unrecorded_publish_received = Hash(UInt16, UInt32).new
      @routing_generation = 0u32
      # Set once a wait fails while open: only a shutdown stops the persister
      @persister_stopped = false
      @deliver_loop_started = false
      @packet_ids_exhausted = false
      # Bumped by every `client=`, so a QoS 2 batch can tell that a reattach
      # requeued what it booked so far
      @attach_count = 0u32
      # The username in the .metadata file, see #persist_username
      @metadata_username : String? = nil
      # This session's topic filters and their QoS, kept by the MQTT exchange
      # as it binds and unbinds them, so finding a session's subscriptions
      # doesn't walk every subscription in the vhost
      getter subscriptions = Hash(String, SubscriptionOptions).new

      # Whether the session is a queue: in the definitions, counted towards
      # max-queues and visible in the API. A connection's session becomes one
      # only once it needs one, see `Sessions#register`.
      getter? registered : Bool

      protected def initialize(@vhost : VHost,
                               @name : String,
                               auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = ARGUMENTS,
                               @registered = true)
        @arguments = arguments
        @session_expiry_interval = self.class.expiry_from(@name, arguments, auto_delete)
        @offline_ttl = @session_expiry_interval
        @durable = !@session_expiry_interval.zero?
        @last_packet_id = 0u16
        @client_id = @name.lchop(SESSION_PREFIX)
        @permission_service = @vhost.mqtt_permission_service
        @inflight = Hash(UInt16, Inflight).new

        @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
        @data_dir = File.join(
          durable? ? @vhost.data_dir : File.join(@vhost.data_dir, "transient"),
          Digest::SHA1.hexdigest(@name)
        )
        @replicator = durable? ? @vhost.@replicator : nil
        @durable_mailbox = durable? ? ::Channel(UInt64).new(1) : nil
        @metadata_file = File.join(@data_dir, ".metadata")
        @permission_context = PermissionService::Context.new(nil, @client_id)
        if @registered
          load_from_disk
        elsif durable?
          # Held ids are recorded from the first QoS 2 PUBLISH, which first
          # makes the session a queue
          @packet_id_log = PacketIdLog.new(File.join(@data_dir, "packet_ids.log"), @replicator, @vhost.persister, read_file: false)
        end
      end

      # Only a queue has state on disk. Until a session is one, a directory
      # under its name can belong to a same-named queue still being deleted,
      # so `#registered!` loads it, once that delete has finished.
      private def load_from_disk : Nil
        if File.exists?(@metadata_file)
          @replicator.try &.register_file(@metadata_file)
          username = @metadata_username = read_metadata_file
          # An attached client's own username takes precedence
          @permission_context = PermissionService::Context.new(username, @client_id) if @permission_context.username.nil?
        end
        load_state
      end

      # Like a queue whose store fails to load, the session is closed rather
      # than raising, so the rest of the vhost still loads. A persistent
      # CONNECT to it is refused, see `Broker#add_client_locked`.
      private def load_state : Nil
        # The store first, as it registers its files with the replicator
        if segments_on_disk?
          store = @msg_store = open_msg_store
          # A bad segment closes the store rather than raising. Its messages
          # are not loaded, so restoring would owe PUBRELs for them.
          if store.closed
            @log.error { "Message store closed while loading, closing session" }
            return close_unloaded
          end
        end
        if durable?
          log = @packet_id_log ||= PacketIdLog.new(File.join(@data_dir, "packet_ids.log"), @replicator, @vhost.persister)
          @awaiting_pubrel.concat(log.awaiting_pubrel)
        end
        restore_publish_sent
        ensure_deliver_loop if message_count.positive? || expires?
      rescue ex
        @log.error(exception: ex) { "Failed to load session, closing it" }
        close_unloaded
      end

      # Files it could not load are registered by path, or a follower's full
      # sync would delete them
      private def close_unloaded : Nil
        if (replicator = @replicator) && Dir.exists?(@data_dir)
          Dir.each_child(@data_dir) do |name|
            path = File.join(@data_dir, name)
            replicator.register_file(path) if File.file?(path)
          end
        end
        close
      end

      # Created by the first subscription, see #subscription_bound, as only a
      # subscription routes messages to a session. Raises rather than creating
      # it, to keep the creation out of the publish path.
      private def msg_store : SessionMessageStore
        @msg_store || raise "Session #{@name} has no message store"
      end

      private def open_msg_store : SessionMessageStore
        raise MessageStore::ClosedError.new if closed?
        # A clean session's directory is wiped at boot, so it needs no fsync
        durable? ? FileSystem.mkdir_p(@data_dir) : Dir.mkdir_p(@data_dir)
        store = SessionMessageStore.new(@data_dir, @replicator, durable?, metadata: @metadata, persister: @vhost.persister)
        store.on_original_packet_id_dropped = ->original_packet_id_dropped(SegmentPosition, UInt16)
        store.on_original_packet_id_released = ->original_packet_id_released(UInt16)
        store
      end

      # The store's first segment is created with it, so a session that ever
      # had a store has one
      private def segments_on_disk? : Bool
        # Checked first: a new session has no directory, and raising for it
        # would cost every CONNECT an exception
        return false unless Dir.exists?(@data_dir)
        Dir.children(@data_dir).any?(&.starts_with?("msgs."))
      rescue File::NotFoundError
        false
      end

      # Started by the first message, as a fiber's stack costs mappings too.
      # Only messages need it: requeues come from deliveries it made.
      private def ensure_deliver_loop : Nil
        return if @deliver_loop_started
        @deliver_loop_started = true
        spawn deliver_loop, name: "Session#deliver_loop"
      end

      def closed?
        @closed.get(:acquire)
      end

      # Remembered like a requeued message's id, so `next_packet_id` skips it
      # and the re-send keeps it [MQTT-4.4.0-1]. A message gone from the store
      # was deleted at PUBREC (or dropped), so only the PUBREL is owed.
      private def restore_publish_sent : Nil
        log = @packet_id_log || return
        log.publish_sent.each do |id, sp|
          if (store = @msg_store) && store.includes?(sp)
            store.remember_original_packet_id(sp, id)
          else
            @inflight[id] = Inflight.new(Inflight::Awaiting::PubComp, nil)
          end
        end
        refresh_capacity
      end

      # Not preallocated: they stay empty until the session gets a message,
      # see #update_rates
      protected def self.stats_log_capacity : Int32
        0
      end

      # A session that never got a message keeps empty stats logs rather than
      # filling 14 of them with zeros: most are publish-only clients'.
      def update_rates : Nil
        return if @publish_log.empty? && @publish_count.get(:relaxed).zero? && message_count.zero?
        previous_def
      end

      def consumer_count : UInt32
        @client.nil? ? 0u32 : 1u32
      end

      def message_count : UInt32
        @msg_store.try(&.size.to_u32) || 0u32
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
        # Its QoS 2 ids are no longer recorded, so an attached client would get
        # PUBRECs that do not survive a restart. A reconnect is refused.
        @client.try &.force_close
        @has_capacity.close
        @has_client.close
        @resuming.close
        @msg_store_lock.synchronize do
          @msg_store.try &.close
        end
        @durable_mailbox.try &.close
        record_publish_received_on_close unless @deleted
        @packet_id_log.try &.close
        true
      end

      # At shutdown the persister closes before the sessions, so the drain
      # these wait for may never come. Write order is enough for a restart
      # and a failover; only power loss can still drop one. Connections close
      # before sessions, so no PUBREL is handled while these are written.
      private def record_publish_received_on_close : Nil
        log = @packet_id_log || return
        @unrecorded_publish_received.each_key { |id| log.publish_received(id) }
        @unrecorded_publish_received.clear
      rescue ex : PacketIdLog::Error
        @log.error(exception: ex) { "Failed to record held QoS 2 packet ids" }
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
        # One that never became a queue wrote nothing, and its directory can
        # belong to a queue imported under the same name since
        return true unless @registered
        @msg_store_lock.synchronize do
          @msg_store.try &.delete
        end
        @packet_id_log.try &.delete
        # A session without a store can still have a directory, for its
        # packet_ids.log
        FileUtils.rm_rf(@data_dir)
        # Every file registered under it, also those `close_unloaded`
        # registered: a stale packet_ids.log left on a follower would be
        # loaded by a new session with this name after a failover
        @replicator.try &.delete_dir(@data_dir)
        @vhost.delete_queue(@name)
        true
      end

      def auto_delete? : Bool
        @session_expiry_interval.zero?
      end

      # Whether the expiry clock has to run while no client is attached
      private def expires? : Bool
        !@session_expiry_interval.zero? && @session_expiry_interval != UInt32::MAX
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

      # Called once `DefinitionsStore#register_session` has released the
      # definitions lock, which a delete of a same-named queue holds until its
      # directory is gone. Nothing yields in between.
      protected def registered! : Nil
        @registered = true
        load_from_disk
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
          # Started for the expiry clock alone, so there may be no store yet.
          # The first publish starts the loop again.
          unless store = @msg_store
            @deliver_loop_started = false
            break
          end
          next wait_for_messages(store) if store.empty?
          next @has_capacity.when_true.receive? unless @has_capacity.value
          get_packet do |pub_packet, bytesize|
            client.send(pub_packet)
            # Per message, also within a QoS 2 batch
            if (delivered_bytes &+= bytesize) > Config.instance.yield_each_delivered_bytes
              delivered_bytes = 0
              Fiber.yield
            end
          end
          # Nothing is confirmed again, so every QoS 2 delivery would park
          break if @persister_stopped
        rescue ex
          @log.error(exception: ex) { "Failed to deliver message in deliver_loop" }
          # Sending yields, so a write can fail after the session was
          # reattached. Close the connection it was written to, not whichever
          # happens to be current.
          client.try &.close("Server force closed client")
          # Needed although `remove_client` also detaches: until it runs, this
          # loop would spin on the dead client. It can still race an attach
          # parked on the store lock; see the attach/detach follow-up in #2236.
          self.client = nil if @client == client
        end
        # Every delete, expiry included, ends the loop here. The session has
        # ended, so a will still waiting out its delay is due [MQTT-3.1.2-8].
        # A plain close (shutdown) drops it (§3.1.2.5).
        publish_pending_will if @deleted
      end

      # A resend keeps the packet id the client already knows [MQTT-4.4.0-1],
      # unless that id is still in flight - reissuing it would overwrite the
      # `@inflight` entry holding it - or is `0`, which may not go on the wire
      # [MQTT-2.2.1-4]. Both fall back to a fresh id. Neither happens today:
      # `free_packet_id` never hands out an id owed to a requeued message, and
      # never 0. The flag tells whether the id is the original one.
      private def packet_id_for(sp : SegmentPosition) : {UInt16, Bool}?
        if id = msg_store.original_packet_id?(sp)
          return {id, true} unless id.zero? || @inflight.has_key?(id)
        end
        next_packet_id.try { |fresh| {fresh, false} }
      end

      # `@has_capacity` mirrors "the in-flight window has room". Recomputed from
      # `@inflight` rather than written as a literal, since it is updated from both
      # the deliver_loop and the client's fiber and a stale `false` parks the
      # deliver_loop with no ack left to reopen the gate. `swap` rather than `set`
      # because this runs per delivery and per ack, and `set` takes both channel
      # locks even when the value is unchanged.
      private def refresh_capacity : Nil
        @has_capacity.swap(@inflight.size < inflight_limit)
      end

      # The client's Receive Maximum caps unacknowledged QoS 1 and 2 PUBLISHes,
      # counting a QoS 2 one until PUBCOMP [MQTT-3.3.4-9], which is exactly what
      # `@inflight` holds. Our own limit still applies when it is lower.
      private def inflight_limit : UInt16
        limit = Config.instance.max_inflight_messages
        if (receive_maximum = @client.try(&.receive_maximum)) && receive_maximum < limit
          limit = receive_maximum
        end
        limit
      end

      # Whether `id` still names this exact delivery. Sending yields, so
      # `client=`, `puback` or `pubrec` can have moved it in the meantime.
      private def booked?(id : UInt16, sp : SegmentPosition) : Bool
        @inflight[id]?.try(&.sp) == sp
      end

      # Parks until there is something to deliver. The detach arm matters: on its
      # own, a park on @msg_store.empty never wakes when the client leaves, so the
      # loop would never reach the top again to start the expiry clock.
      private def wait_for_messages(store : SessionMessageStore) : Nil
        select
        when store.empty.when_false.receive?
        when @has_client.when_false.receive?
        end
      end

      # Parks until a client attaches, the session expires, or a pending will
      # is due. This is the only place the expiry clock runs - exactly the
      # window in which the session has no connection (§3.1.2.11.2). Measured
      # from `@offline_since`, so re-entering the wait does not restart it.
      private def wait_for_client : Nil
        return wait_for_claim if @resuming.value
        ttl = @offline_ttl
        # 0 needs no timer: `Broker#remove_client` ends that session with its
        # connection, under the client-id lock.
        expires_at = @offline_since + ttl.seconds unless ttl.zero? || ttl == UInt32::MAX
        deadline = expires_at
        # A tie goes to expiry, which publishes the will on its way out.
        if (will_at = @pending_will.try &.deadline) && (deadline.nil? || will_at < deadline)
          deadline = will_at
        end
        if deadline.nil?
          @has_client.when_true.receive?
          return
        end
        select
        when @has_client.when_true.receive?
        when timeout({deadline - Time.instant, Time::Span.zero}.max)
          deadline == expires_at ? expire : publish_pending_will
        end
      end

      # Runs on the session's own fiber. `delete` closes @has_client and the
      # message store, so deliver_loop's `break if closed?` exits on the next
      # pass; the re-entrant q.delete from @vhost.delete_queue is a no-op via
      # @deleted.
      # Parks until the claiming connection attaches or goes away.
      private def wait_for_claim : Nil
        select
        when @has_client.when_true.receive?
        when @resuming.when_false.receive?
        end
      end

      private def expire : Nil
        # The timer can fire just before a reconnect claims the session, and
        # this fiber only runs after that connection's CONNACK.
        return if @resuming.value || @client
        @log.info { "Session expired after #{@session_expiry_interval}s offline" }
        delete
      end

      # The closing connection's delayed will. A deleted session has ended, so
      # the will is due now [MQTT-3.1.2-8]: this fiber may already have exited.
      def arm_will(will : PendingWill) : Nil
        if @deleted
          publish_will(will)
        else
          @pending_will = will
          # The timer runs in the deliver_loop, which a session with nothing
          # to deliver has not started
          ensure_deliver_loop
        end
      end

      # A connection opened for this session: it cancels the will
      # [MQTT-3.1.3-9] and holds off expiry until it attaches or goes away.
      def resume : Nil
        @pending_will = nil
        @resuming.set(true)
      end

      private def publish_pending_will : Nil
        will = @pending_will || return
        @pending_will = nil
        publish_will(will)
      end

      private def publish_will(will : PendingWill) : Nil
        # A fresh context: this fiber publishes no other way
        will.broker.publish(will.packet, @name, PublishContext.new)
      rescue ex
        @log.warn { "Failed to publish will: #{ex.message}" }
      end

      def client : MQTT::Client?
        @client
      end

      # A takeover's `Client#close` joins the old read fiber before the new
      # `Client#run` reaches this, so no `puback`/`pubrec` of the old client is
      # in flight while `@inflight` is walked.
      def client=(client : MQTT::Client?)
        # A closed store can't be touched, but `delete` still has to know which
        # connection to close.
        return @client = client if closed?
        @attach_count &+= 1
        @last_get_time = RoughTime.instant

        # Ids past PUBREC, which owe a PUBREL rather than a message.
        awaiting_pubcomp = Array(UInt16).new

        # Only a session with a non-zero expiry outlives its connection
        # [MQTT-3.1.2-23]. It requeues what it owes and remembers the packet ids, to
        # resend under the ids the client already knows [MQTT-4.4.0-1].
        if durable?
          @msg_store_lock.synchronize do
            @inflight.each do |packet_id, inflight|
              if sp = inflight.sp
                msg_store.remember_original_packet_id(sp, packet_id)
                msg_store.requeue(sp)
              else
                awaiting_pubcomp << packet_id
              end
            end
          end
        end

        @inflight.clear
        @unacked_count.set(0, :release)
        @unacked_bytesize.set(0, :release)

        # Re-booked even when detaching: doing it only for an attached client
        # would drop the obligation on the disconnect it exists to survive.
        awaiting_pubcomp.each { |id| @inflight[id] = Inflight.new(Inflight::Awaiting::PubComp, nil) }
        refresh_capacity

        # Assigned before the writes below, which yield: `Session#publish`
        # drops a QoS 0 message while it is nil.
        # A detach, or a claim that ends without attaching, starts the offline
        # window with the interval of this moment: a DISCONNECT has applied its own.
        if client.nil? && (@client || @resuming.value)
          @offline_since = Time.instant
          @offline_ttl = @session_expiry_interval
        end
        @client = client
        # After the assignment: the window depends on the client's Receive
        # Maximum.
        refresh_capacity
        unless client.nil? || awaiting_pubcomp.empty?
          # Queued, so each leaves once the delete at its PUBREC is durable;
          # `Client#run` attaches after CONNACK. [MQTT-4.4.0-1] does not order
          # them against the replayed PUBLISHes.
          @log.info { "resending #{awaiting_pubcomp.size} PUBREL" }
          awaiting_pubcomp.each { |id| client.queue_ack(Client::PendingAck::PacketType::PubRel, id) }
        end
        @unknown_pubrecs.each { |id| client.queue_ack(Client::PendingAck::PacketType::PubRel, id) } if client
        @resuming.set(false)
        @has_client.set(!client.nil?)
        # The expiry clock and a pending will's timer run in the deliver_loop,
        # which a session with nothing to deliver has not started
        ensure_deliver_loop if client.nil? && @registered && (expires? || @pending_will)
        if client && (username = client.user.name) != @permission_context.username
          @permission_context = PermissionService::Context.new(username, @client_id)
        end
        persist_username if client

        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable? : Bool
        @durable
      end

      # The .metadata file holds the last attached username, so a restored
      # session keeps its member rules. It only exists for a durable session a
      # client has attached to; nothing else has a username to restore.
      # Anything that is not a JSON object with a string username is treated
      # as an unknown user; a bad file must never stop the session from loading.
      private def read_metadata_file : String?
        JSON.parse(File.read(@metadata_file)).as_h?.try(&.["username"]?).try(&.as_s?)
      rescue ex : JSON::ParseException | IO::Error
        @log.warn(exception: ex) { "Could not read #{@metadata_file}, session user unknown until a client connects" }
        nil
      end

      # The username decides which messages a session gets while no client is
      # attached, after a restart too. Only a session with a subscription gets
      # messages, so it is kept from the first one on, which also created the
      # session's store: a publish-only client's session writes no file.
      private def persist_username : Nil
        return unless durable? && @msg_store
        username = @permission_context.username || return
        return if username == @metadata_username
        write_metadata_file(username)
        @metadata_username = username
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

      # What a SUBSCRIBE did to the subscription for one topic filter
      enum SubscribeResult
        New
        # Existence is by topic filter alone: [MQTT-3.8.4-3] replaces a
        # subscription whose filter is identical, so a re-subscribe that
        # changes the QoS or the options is a replacement, not a new one.
        Replaced
        # A concurrent API deletion can make the bind fail
        Failed
      end

      # TODO: Check session identity under the definitions lock before binding.
      def subscribe(tf, options : SubscriptionOptions) : SubscribeResult
        if current = @subscriptions[tf]?
          return SubscribeResult::Replaced if current == options
          unbind(tf, MQTT.subscription_arguments(current))
        end
        return SubscribeResult::Failed unless @vhost.bind_queue(@name, EXCHANGE, tf, MQTT.subscription_arguments(options))
        current ? SubscribeResult::Replaced : SubscribeResult::New
      end

      # Returns whether a matching subscription existed, so the v5 UNSUBACK can
      # report Success vs NoSubscriptionExisted per topic filter [MQTT-3.11.3-2].
      def unsubscribe(tf) : Bool
        if options = @subscriptions[tf]?
          unbind(tf, MQTT.subscription_arguments(options))
          true
        else
          false
        end
      end

      def deliver(msg : Message, filter : String, ctx : PublishContext) : Bool
        publish(msg)
      end

      # Called for every binding, whether from a SUBSCRIBE, the HTTP API, a
      # definitions import or a restore at startup
      #
      # False if the store cannot be created, e.g. at `vm.max_map_count`, so the
      # bind fails: a SUBSCRIBE gets Failure and a restore skips the binding.
      protected def subscription_bound(filter : String, options : SubscriptionOptions) : Bool
        unless closed?
          begin
            # Creating the store here keeps it out of the publish that first
            # routes to it, which a fanout to many new subscribers would pay for
            # each of them. First, so a failure leaves the subscription unrecorded.
            @msg_store_lock.synchronize { @msg_store ||= open_msg_store }
            persist_username
          rescue ex
            @log.error(exception: ex) { "Could not bind '#{filter}'" }
            return false
          end
        end
        @subscriptions[filter] = options
        true
      end

      protected def subscription_unbound(filter : String) : Nil
        @subscriptions.delete(filter)
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
          # Again under the lock: the fiber that held it can close the session
          # before this one runs, and raising here would fail the whole fan-out
          return false if closed?
          msg_store.push(msg)
          drop_overflow
          ensure_deliver_loop
        end
        @publish_count.add(1, :relaxed)
        true
      end

      def bindings
        @vhost.queue_bindings(self)
      end

      private def unbind(rk, arguments)
        @vhost.unbind_queue(@name, EXCHANGE, rk, arguments || AMQP::Table.new)
      end

      private def get_packet(& : Protocol::Publish, UInt32 -> Nil) : Nil
        raise ClosedError.new if closed?
        # The payload is sent straight from the segment, which a close or
        # delete of the session can unmap while the send is suspended, so it
        # stays leased until sent
        env = @msg_store_lock.synchronize { msg_store.shift_leased? } || return
        if expired_undelivered?(env)
          delete_message(env.segment_position)
          env.release
          return
        end
        if qos2_batched?(env)
          deliver_qos2_batch(env) { |packet, bytesize| yield packet, bytesize }
          return
        end
        begin
          sp = env.segment_position
          # `nil` counts as QoS 0: `build_packet` maps it to 0, so booking an
          # id would leak the slot. Nothing produces a nil today.
          delivery_mode = env.message.properties.delivery_mode
          if delivery_mode.nil? || delivery_mode.zero?
            deliver_no_ack(env, sp) { |packet, bytesize| yield packet, bytesize }
          else
            deliver_acked(env, sp) { |packet, bytesize| yield packet, bytesize }
          end
        ensure
          env.release
        end
      rescue ex : MessageStore::Error
        @log.error(ex) { "Queue closed due to error" }
        close
        raise ClosedError.new(cause: ex)
      end

      # A durable session's QoS 2 PUBLISH waits for its packet id record to be
      # durable, since a subscriber holding an id we forgot dedupes the next
      # message under it [MQTT-4.3.3-2]
      private def qos2_batched?(env : Envelope) : Bool
        return false unless @packet_id_log
        delivery_mode = env.message.properties.delivery_mode
        !delivery_mode.nil? && delivery_mode >= 2u8
      end

      MAX_QOS2_BATCH = 128

      # Up to `MAX_QOS2_BATCH` durable QoS 2 PUBLISHes are recorded, wait for
      # one drain together, then are sent, so the drain is not paid per
      # message.
      private def deliver_qos2_batch(first : Envelope, & : Protocol::Publish, UInt32 -> Nil) : Nil
        batch = @qos2_batch
        sent = 0
        begin
          collect_qos2_batch(first)
          return if batch.empty?
          unless wait_until_durable
            batch.each { |(e, id, _)| unsend_on_stopped_persister(id, e.segment_position) } unless closed?
            return
          end
          batch.each do |(e, id, original)|
            sp = e.segment_position
            # `client=` requeued it while we waited; the new connection sends it
            if booked?(id, sp)
              yield build_packet(e, id, original), sp.bytesize
              count_delivery(e)
              msg_store.forget_original_packet_id(sp) if booked?(id, sp)
            end
            sent += 1
          end
          refresh_capacity
        rescue ex
          # Back in the store, as in `deliver_acked`. A closed store has nothing
          # to requeue into.
          unless closed?
            batch.each(start: sent, count: batch.size - sent) do |(e, id, _)|
              sp = e.segment_position
              unsend(id, sp) if booked?(id, sp)
            end
          end
          raise ex
        ensure
          batch.each { |(e, _, _)| e.release }
          # A burst's buffer is not kept by an idle session, but is reused
          # while there is more to send
          if batch.size > 16 && @msg_store.try(&.empty?) != false
            @qos2_batch = Array({Envelope, UInt16, Bool}).new
          else
            batch.clear
          end
        end
      end

      # Books and records each message into `@qos2_batch`. Only a message that
      # joins is shifted: one that does not stays first in the store, for the
      # deliver_loop to send next, in order and to the current client.
      private def collect_qos2_batch(env : Envelope) : Nil
        batch = @qos2_batch
        attached = @attach_count
        sp = env.segment_position
        id, original = packet_id_for(sp) || begin
          env.release
          requeue_without_packet_id(sp)
          return
        end
        loop do
          # The size check is gated, so a client without a limit builds no
          # extra packet
          if expired_undelivered?(env) ||
             (@client.try(&.max_packet_size) && exceeds_max_packet_size?(build_packet(env, id, original)))
            # Never booked, so not redelivered [MQTT-3.1.2-25]
            delete_message(sp)
            env.release
          else
            # Booked before the record and the send, see `deliver_acked`
            book(id, sp, Inflight::Awaiting::PubRec)
            batch << {env, id, original}
            log_packet_id &.publish_sent(id, sp)
            return if batch.size >= MAX_QOS2_BATCH
          end
          # A re-send keeps its remembered id without asking `next_packet_id`,
          # so the window is checked here too
          return if @inflight.size >= inflight_limit
          env, id, original = next_qos2_batched(attached) || return
          sp = env.segment_position
        end
      end

      # The next message, leased, with its packet id, if it can join the batch.
      # Its id is taken before it is shifted: one requeued for lack of an id
      # would go out marked as a re-send. Stops once a reattach requeued what
      # is booked, which shifting again would put in the batch twice.
      private def next_qos2_batched(attached : UInt32) : {Envelope, UInt16, Bool}?
        @msg_store_lock.synchronize do
          return unless @attach_count == attached
          next_env = msg_store.first?
          return unless next_env && qos2_batched?(next_env)
          id, original = packet_id_for(next_env.segment_position) || return
          env = msg_store.shift_leased? || return
          {env, id, original}
        end
      end

      private def book(id : UInt16, sp : SegmentPosition, awaiting : Inflight::Awaiting) : Nil
        @inflight[id] = Inflight.new(awaiting, sp)
        @unacked_count.add(1, :relaxed)
        @unacked_bytesize.add(sp.bytesize, :relaxed)
      end

      private def count_delivery(env : Envelope) : Nil
        if env.redelivered
          @redeliver_count.add(1, :relaxed)
        else
          @deliver_count.add(1, :relaxed)
          @deliver_get_count.add(1, :relaxed)
        end
      end

      private def deliver_no_ack(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Nil
        begin
          packet = build_packet(env, nil)
          if exceeds_max_packet_size?(packet)
            delete_message(sp)
            return
          end
          yield packet, sp.bytesize
          if env.redelivered
            @redeliver_count.add(1, :relaxed)
          else
            @deliver_no_ack_count.add(1, :relaxed)
            @deliver_get_count.add(1, :relaxed)
          end
        rescue ex # requeue failed delivery
          @msg_store_lock.synchronize { msg_store.requeue(sp) }
          raise ex
        end
        delete_message(sp)
      end

      # Without a free packet id the message is requeued for the next attempt
      private def deliver_acked(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Nil
        id, original = packet_id_for(sp) || begin
          requeue_without_packet_id(sp)
          return
        end
        # Raises before anything is booked, which the rescue below would not
        # roll back. Unreachable today, but being wrong loses the message.
        packet = begin
          build_packet(env, id, original)
        rescue ex
          @msg_store_lock.synchronize { msg_store.requeue(sp) }
          raise ex
        end
        if exceeds_max_packet_size?(packet)
          # Discard without sending and complete the delivery: the id is never
          # booked, so it is not redelivered [MQTT-3.1.2-25].
          delete_message(sp)
          return
        end
        begin
          # Booked before the send, which yields: the client can acknowledge
          # before we return, and an acknowledgement finding no entry is either
          # fatal (`puback`) or silently dropped (`pubrec`).
          book(id, sp, packet.qos == 1u8 ? Inflight::Awaiting::PubAck : Inflight::Awaiting::PubRec)
          yield packet, sp.bytesize
          count_delivery(env)
          # `client=` may have requeued `sp` and remembered `id` during the
          # send; forgetting then costs the redelivery its id [MQTT-4.4.0-1].
          msg_store.forget_original_packet_id(sp) if booked?(id, sp)
          refresh_capacity
        rescue ex # requeue failed delivery
          # Roll back only what is still ours: requeueing an entry `client=`
          # already requeued hands the message out twice.
          unsend(id, sp) if booked?(id, sp)
          raise ex
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

      private def requeue_without_packet_id(sp : SegmentPosition) : Nil
        @msg_store_lock.synchronize { msg_store.requeue(sp) }
        # Without this the deliver_loop spins: the store is non-empty and
        # capacity still reads true. Recomputed after the lock, which can
        # park while an ack frees a slot or an id; no yield from here on.
        refresh_capacity
        return if free_packet_id
        # The window has room, but ids owed to requeued messages hold the
        # rest. Parked until an acknowledgement frees one.
        # Logged once per pause: every ack reopens the gate for another try
        @log.warn { "No free packet id, delivery paused" } unless @packet_ids_exhausted
        @packet_ids_exhausted = true
        @has_capacity.swap(false)
      end

      # Cumulative confirms suit a single waiter, the deliver_loop: any id at
      # or past our seq covers it, and an older one left in the mailbox is
      # skipped. False when the persister stopped or the session closed.
      private def wait_until_durable : Bool
        mailbox = @durable_mailbox || return false
        seq = @durable_seq &+= 1
        return false unless @vhost.enqueue_ack(self, seq)
        while confirmed = mailbox.receive?
          return !closed? if confirmed >= seq
        end
        false
      end

      # Called from the persister's thread
      def enqueue_confirm_ack(msgid : UInt64) : Nil
        Persister::ConfirmTarget.offer(@durable_mailbox, msgid)
      end

      # Back in the store under `id`, which the log already holds, so a
      # restart re-sends it as it would any unacknowledged PUBLISH.
      private def unsend_on_stopped_persister(id : UInt16, sp : SegmentPosition) : Nil
        @persister_stopped = true
        return unless booked?(id, sp)
        unsend(id, sp)
        refresh_capacity
      end

      # Back in the store under `id`, as `client=` does: the client may hold the
      # id, and a QoS 2 one is in the packet id log [MQTT-4.4.0-1].
      private def unsend(id : UInt16, sp : SegmentPosition) : Nil
        @inflight.delete(id)
        # Before the lock, which can park: `client=` zeroes both counters,
        # and a `sub` after that wraps an unsigned atomic.
        @unacked_count.sub(1, :relaxed)
        @unacked_bytesize.sub(sp.bytesize, :relaxed)
        @msg_store_lock.synchronize do
          msg_store.remember_original_packet_id(sp, id)
          msg_store.requeue(sp)
        end
      end

      # [MQTT-3.3.2-5] deletes an expired message only while onward delivery has
      # not started. A remembered packet id means it was sent before and requeued
      # on reattach, so [MQTT-4.4.0-1] still owes it. Not `env.redelivered`: a
      # message requeued for want of a packet id was never sent.
      private def expired_undelivered?(env) : Bool
        msg = env.message
        return false unless PublishHeaders.expired?(msg.properties.headers, msg.timestamp)
        return false unless (msg_store.original_packet_id?(env.segment_position) || 0u16).zero?
        @log.debug { "Dropping message past its Message Expiry Interval" }
        true
      end

      # A message loaded from disk is not `redelivered`, so a re-send under the
      # original id is marked by `dup` [MQTT-3.3.1-1].
      def build_packet(env, packet_id, dup = false) : Protocol::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.[RETAIN_HEADER]? == true
        # `delivery_mode` is read off disk unvalidated and `Publish.new` raises
        # above QoS 2, which would make one bad byte a poison message.
        qos = MQTT.granted_qos(msg.properties.delivery_mode)
        dup = qos.zero? ? false : (dup || env.redelivered)
        # IO::Framing::V3#write_properties discards these, so a v3 subscriber
        # should not pay six Table#fetch linear scans per delivery to build them.
        properties = if @client.try(&.version.v5?)
                       PublishHeaders.restore(msg.properties.headers)
                     else
                       Protocol::PublishProperties.new
                     end
        if interval = properties.message_expiry_interval
          # Less the time spent here [MQTT-3.3.2-6]. 0 for an expired message
          # we still owe, and never above the original if the clock went back.
          remaining = interval.to_i64 - (RoughTime.unix_ms - msg.timestamp) // 1000
          properties.message_expiry_interval = remaining.clamp(0_i64, interval.to_i64).to_u32
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

      def puback(packet : Protocol::PubAck) : Nil
        id = packet.packet_id
        inflight = @inflight[id]?
        if inflight.nil?
          # Owed to a requeued message: a client acking a message it got on its
          # previous connection. It is re-sent and acked again, as for `pubrec`.
          if @msg_store.try(&.original_packet_id_in_use?(id))
            @log.debug { "PUBACK for packet id '#{id}', which is not in flight" }
            return
          end
          # Nothing re-sends a PUBACK, so an unknown one is a protocol error
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBACK for unknown packet id '#{id}'")
        end
        sp = inflight.sp
        # A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-3], so a PUBACK for
        # one is a protocol violation. Checked before the delete, so it cannot
        # drop an obligation the session still owes.
        unless inflight.awaiting.pub_ack? && sp
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBACK for packet id '#{id}', which is awaiting a QoS 2 acknowledgement")
        end
        @inflight.delete(id)
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
      # Returns rather than raises for an unknown id: a clean session's window
      # does not survive a restart, a client may still answer for it, and
      # raising would publish its will. It is answered with PUBREL, the only
      # packet that lets the client release the id, as an unknown PUBREL is
      # answered with PUBCOMP.
      def pubrec(packet : Protocol::PubRec) : Bool
        id = packet.packet_id
        unless inflight = @inflight[id]?
          # An id owed to a requeued message is not unknown: its PUBLISH is
          # about to be re-sent under it, and a PUBREL now would let the client
          # take that re-send for a new message.
          if id.zero? || @msg_store.try(&.original_packet_id_in_use?(id))
            @log.debug { "PUBREC for packet id '#{id}', which is not in flight" }
          else
            @log.debug { "PUBREC for unknown packet id '#{id}', answering PUBREL" }
            # Held until PUBCOMP: the PUBREL waits for a drain, and a new
            # PUBLISH under this id before it would be released in its place.
            @unknown_pubrecs << id
            send_pubrel(id)
          end
          return false
        end
        if inflight.awaiting.pub_comp?
          # A repeat of a PUBREC we already answered, so our PUBREL was lost.
          # Answering again is the only way the client can release the id.
          send_pubrel(id)
          return false
        end
        unless inflight.awaiting.pub_rec?
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBREC for QoS #{inflight.qos} packet id '#{id}'")
        end
        sp = inflight.sp.as(SegmentPosition)
        # A reason code of 0x80 or above refuses the message, which ends the
        # delivery like a PUBACK: the id is freed and no PUBREL follows
        # [MQTT-4.3.3-4]. Only v5 PUBREC carries one.
        refused = packet.reason_code.value >= 0x80
        # Before the send: a failed write still leaves the correct state, and
        # `client=` re-sends the PUBREL.
        if refused
          @inflight.delete(id)
          # Ends the exchange, so the delete below is not a drop: no PUBREL is
          # owed even if a re-send under the original id is still being written.
          msg_store.forget_original_packet_id(sp)
        else
          @inflight[id] = Inflight.new(Inflight::Awaiting::PubComp, nil)
        end
        @ack_count.add(1, :relaxed)
        @unacked_count.sub(1, :relaxed)
        @unacked_bytesize.sub(sp.bytesize, :relaxed)
        delete_message(sp)
        if refused
          refresh_capacity
        else
          @msg_store_lock.synchronize { msg_store.mark_delete_dirty(sp) }
          send_pubrel(id)
          # No `refresh_capacity`: the id is still booked, so the window is
          # unchanged.
        end
        true
      end

      def pubcomp(packet : Protocol::PubComp) : Bool
        id = packet.packet_id
        unless inflight = @inflight[id]?
          if @unknown_pubrecs.delete(id)
            # Can reopen a delivery paused for lack of a free id
            refresh_capacity
          else
            @log.warn { "PUBCOMP for unknown packet id '#{id}'" }
          end
          return false
        end
        unless inflight.awaiting.pub_comp?
          raise ProtocolViolation.new(Protocol::Disconnect::ReasonCode::ProtocolError, "PUBCOMP for packet id '#{id}' that has not been PUBRECed")
        end
        # No wait: a lost record costs one spare PUBREL, answered with PUBCOMP.
        # Before the id is freed: the write can yield, and a reuse recorded
        # first would be deleted by this record on restart.
        log_packet_id &.pubcomp_received(id)
        @inflight.delete(id)
        # Load-bearing: for a window full of ids awaiting PUBCOMP, this is the
        # only event that can reopen the capacity gate.
        refresh_capacity
        true
      end

      # Records `packet_id`, returning false if it was already held, i.e. this
      # PUBLISH is a re-send of one already routed.
      def publish_received(packet_id : UInt16) : Bool
        return false if @awaiting_pubrel.includes?(packet_id)
        if @awaiting_pubrel.size >= Config.instance.max_awaiting_pubrel
          raise AwaitingPubrelLimitReached.new("Holding #{@awaiting_pubrel.size} QoS 2 packet ids, max_awaiting_pubrel is #{Config.instance.max_awaiting_pubrel}")
        end
        @awaiting_pubrel.add(packet_id)
        true
      end

      # After routing `packet_id`: the generation its PUBREC's barrier records
      # against, nil when the PUBREC need not wait (a clean session). Tracked
      # until written, so a close can write it if the confirm never comes.
      def publish_routed(packet_id : UInt16) : UInt32?
        return unless durable?
        generation = @routing_generation &+= 1
        @unrecorded_publish_received[packet_id] = generation
        generation
      end

      # Called by the ack writer once the routed message is durable, so the id
      # is never durable without the message it dedupes. Skipped if a PUBREL
      # already released it (a client that did not wait for our PUBREC), or
      # if the id was routed again since: that routing has its own barrier.
      def record_publish_received(packet_id : UInt16, generation : UInt32) : Nil
        return unless @unrecorded_publish_received[packet_id]? == generation
        @unrecorded_publish_received.delete(packet_id)
        log_packet_id &.publish_received(packet_id)
      end

      # Undoes `publish_received` when routing failed. Memory only: nothing was
      # recorded for the id yet.
      def forget_publish_received(packet_id : UInt16) : Nil
        @awaiting_pubrel.delete(packet_id)
      end

      # Releases `packet_id` on PUBREL. False if we were not holding it.
      # The log is written before the id is released, so a failed write
      # leaves it held in both. An unrecorded id is not in the log; it is
      # dropped first, so a barrier cannot record it while the write yields.
      def pubrel_received(packet_id : UInt16) : Bool
        unrecorded = @unrecorded_publish_received.delete(packet_id)
        held = @awaiting_pubrel.includes?(packet_id)
        log_packet_id &.pubrel_received(packet_id) if held && !unrecorded
        @awaiting_pubrel.delete(packet_id)
      end

      # The log raises once closed, and a client's ack writer or read fiber can
      # still be running when the session closes under it.
      private def log_packet_id(& : PacketIdLog ->) : Nil
        return if closed?
        log = @packet_id_log || return
        yield log
      rescue ex : PacketIdLog::Error
        raise ex unless closed?
      end

      # The client may hold a QoS 2 id until our PUBREL [MQTT-4.3.3-2], so a
      # dropped message's id is released, not freed. A QoS 1 id is held by
      # nobody once requeued. Also fires for the delete of an acknowledgement
      # racing the re-send; the id is then still booked (`pubrec` rebooks it
      # as awaiting PUBCOMP before deleting), which is not a drop.
      private def original_packet_id_dropped(sp : SegmentPosition, id : UInt16) : Bool
        return false if @inflight.has_key?(id)
        unless msg_store[sp].properties.delivery_mode == 2u8
          # The id is free, which can reopen a delivery paused for lack of one
          refresh_capacity
          return false
        end
        @inflight[id] = Inflight.new(Inflight::Awaiting::PubComp, nil)
        refresh_capacity
        true
      end

      # After the delete is written and marked dirty, so the PUBREL leaves
      # once it is durable, as at PUBREC.
      private def original_packet_id_released(id : UInt16) : Nil
        send_pubrel(id)
      end

      # Through the ack writer, after the PUBREC's delete is durable. With no
      # client the id stays booked, so the next attach re-sends [MQTT-4.4.0-1].
      private def send_pubrel(id : UInt16) : Nil
        @client.try &.queue_ack(Client::PendingAck::PacketType::PubRel, id)
      end

      private def next_packet_id : UInt16?
        # `>=` not `==`: the limit is mutable at runtime, so the window can
        # already be over it.
        return if @inflight.size >= inflight_limit
        id = free_packet_id || return
        @packet_ids_exhausted = false
        @last_packet_id = id
      end

      private def free_packet_id : UInt16?
        next_id = @last_packet_id
        # Tries each of 1..65535 once, `@last_packet_id` itself last, as it can
        # be the only id left. Packet id 0 is illegal [MQTT-2.2.1-4].
        UInt16::MAX.times do
          next_id = next_id == UInt16::MAX ? 1u16 : next_id + 1u16
          # Skips ids owed to requeued messages too: taking one makes its
          # message fall back to a fresh id, while the client still holds the
          # old one [MQTT-4.4.0-1].
          next if @inflight.has_key?(next_id) || @unknown_pubrecs.includes?(next_id)
          next if @msg_store.try(&.original_packet_id_in_use?(next_id))
          return next_id
        end
      end

      private def delete_message(sp : SegmentPosition) : Nil
        @msg_store_lock.synchronize do
          msg_store.delete(sp)
        end
      end

      private def drop_overflow : Nil
        return unless (ml = @max_length) || (mlb = @max_length_bytes)
        store = @msg_store || return
        if ml = @max_length
          @msg_store_lock.synchronize do
            while store.size > ml
              env = store.shift? || break
              delete_message(env.segment_position)
            end
          end
        end
        if mlb = @max_length_bytes
          @msg_store_lock.synchronize do
            while store.bytesize > mlb
              env = store.shift? || break
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
        count = @msg_store_lock.synchronize { @msg_store.try(&.purge(max_count)) || 0u32 }
        @log.info { "Purged #{count} messages" }
        count
      end

      def in_use? : Bool
        !(message_count.zero? && @client.nil?)
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
        ready = message_count
        ready_bytes = @msg_store.try(&.bytesize) || 0u64
        {
          name:                         @name,
          durable:                      durable?,
          exclusive:                    false,
          auto_delete:                  auto_delete?,
          arguments:                    NamedTuple.new, # "empty" AMQP::Table
          consumers:                    consumer_count,
          vhost:                        @vhost.name,
          messages:                     ready + stats[:messages_unacknowledged],
          total_bytes:                  ready_bytes + stats[:message_bytes_unacknowledged],
          messages_persistent:          durable? ? ready + stats[:messages_unacknowledged] : 0,
          ready:                        ready,
          messages_ready:               ready,
          ready_bytes:                  ready_bytes,
          message_bytes_ready:          ready_bytes,
          ready_avg_bytes:              @msg_store.try(&.avg_bytesize) || 0u32,
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
