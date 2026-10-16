require "../filesystem"
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
require "./permission_service"
require "./session_message_store"
require "./subscriber"
require "./packet_id_log"
require "../persister"

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      # A known packet id acknowledged with the wrong packet type. The client
      # must be disconnected [MQTT-4.8.0-1]; an unknown id is not this, because
      # QoS 1 ids, and a clean session's, do not survive a restart.
      class ProtocolViolation < MQTT::Error; end

      # 3.1.1 has no way to refuse one publish, so going over the cap closes
      # the connection; the client re-sends its PUBRELs on reconnect.
      class AwaitingPubrelLimitReached < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      include MQTT::Subscriber
      include Persister::ConfirmTarget
      Log = ::LavinMQ::Log.for "mqtt.session"

      ARGUMENTS      = AMQP::Table.new({"x-queue-type" => "mqtt"})
      EFFECTIVE_ARGS = {"x-queue-type"}

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
      getter? auto_delete

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
      # Packet ids of QoS 2 PUBLISHes answered with PUBREC and not yet released.
      # Holding the id is the whole of the guarantee: a re-sent PUBLISH carrying
      # one is answered again and not routed twice [MQTT-4.3.3-2]. Not derived
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
      # Routed inbound ids whose PUBLISH_RECEIVED record waits for a drain, by
      # routing generation: a barrier left by an older connection must not
      # record an id the client released and reused since.
      @unrecorded_publish_received = Hash(UInt16, UInt32).new
      @routing_generation = 0u32
      # Set once a wait fails while open: only a shutdown stops the persister
      @persister_stopped = false
      @deliver_loop_started = false
      @packet_ids_exhausted = false
      # The username in the .metadata file, see #persist_username
      @metadata_username : String? = nil
      # This session's topic filters and their QoS, kept by the MQTT exchange
      # as it binds and unbinds them, so finding a session's subscriptions
      # doesn't walk every subscription in the vhost
      getter subscriptions = Hash(String, UInt8).new

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
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
        username = nil
        if File.exists?(@metadata_file)
          @replicator.try &.register_file(@metadata_file)
          username = @metadata_username = read_metadata_file
        end
        @permission_context = PermissionService::Context.new(username, @client_id)
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
          log = @packet_id_log = PacketIdLog.new(File.join(@data_dir, "packet_ids.log"), @replicator, @vhost.persister)
          @awaiting_pubrel.concat(log.awaiting_pubrel)
        end
        restore_publish_sent
        ensure_deliver_loop unless message_count.zero?
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
        ARGUMENTS
      end

      def close : Bool
        return false if @closed.swap(true)
        # Its QoS 2 ids are no longer recorded, so an attached client would get
        # PUBRECs that do not survive a restart. A reconnect is refused.
        @client.try &.force_close
        @has_capacity.close
        @has_client.close
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
        @msg_store_lock.synchronize do
          @msg_store.try &.delete
        end
        @packet_id_log.try &.delete
        # A session without a store can still have a directory, for its
        # packet_ids.log
        FileUtils.rm_rf(@data_dir)
        @replicator.try &.delete_file(@metadata_file)
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
          next msg_store.empty.when_false.receive? if msg_store.empty?
          next @has_client.when_true.receive? if client.nil?
          next @has_capacity.when_true.receive? unless @has_capacity.value
          get_packet do |pub_packet, bytesize|
            client.send(pub_packet)
            delivered_bytes &+= bytesize
          end
          # Nothing is confirmed again, so every QoS 2 delivery would park
          break if @persister_stopped
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
          # Needed although `remove_client` also detaches: until it runs, this
          # loop would spin on the dead client. It can still race an attach
          # parked on the store lock; see the attach/detach follow-up in #2236.
          self.client = nil if @client == client
        end
      end

      # A resend keeps the packet id the client already knows [MQTT-4.4.0-1],
      # unless that id is still in flight - reissuing it would overwrite the
      # `@inflight` entry holding it - or is `0`, which may not go on the wire
      # [MQTT-2.3.1-1]. Both fall back to a fresh id. Neither happens today:
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
        @has_capacity.swap(@inflight.size < Config.instance.max_inflight_messages)
      end

      # Whether `id` still names this exact delivery. Sending yields, so
      # `client=`, `puback` or `pubrec` can have moved it in the meantime.
      private def booked?(id : UInt16, sp : SegmentPosition) : Bool
        @inflight[id]?.try(&.sp) == sp
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
        @last_get_time = RoughTime.instant

        # Ids past PUBREC, which owe a PUBREL rather than a message.
        awaiting_pubcomp = Array(UInt16).new

        # A clean session carries nothing between connections [MQTT-3.1.2-6]. A
        # persistent one requeues what it owes and remembers the packet ids, to
        # resend under the ids the client already knows [MQTT-4.4.0-1].
        unless clean_session?
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
        @client = client
        unless client.nil? || awaiting_pubcomp.empty?
          # Queued, so each leaves once the delete at its PUBREC is durable;
          # `Client#run` attaches after CONNACK. [MQTT-4.4.0-1] does not order
          # them against the replayed PUBLISHes.
          @log.info { "resending #{awaiting_pubcomp.size} PUBREL" }
          awaiting_pubcomp.each { |id| client.queue_ack(Client::PendingAck::PacketType::PubRel, id) }
        end
        @unknown_pubrecs.each { |id| client.queue_ack(Client::PendingAck::PacketType::PubRel, id) } if client
        @has_client.set(!client.nil?)
        if client && (username = client.user.name) != @permission_context.username
          @permission_context = PermissionService::Context.new(username, @client_id)
        end
        persist_username if client

        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable?
        !clean_session?
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

      # A concurrent API deletion can make the bind fail.
      # TODO: Check session identity under the definitions lock before binding.
      def subscribe(tf, qos) : Bool
        if current = @subscriptions[tf]?
          return true if current == qos
          unbind(tf, MQTT.qos_arguments(current))
        end
        @vhost.bind_queue(@name, EXCHANGE, tf, MQTT.qos_arguments(qos))
      end

      def unsubscribe(tf)
        if qos = @subscriptions[tf]?
          unbind(tf, MQTT.qos_arguments(qos))
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
      protected def subscription_bound(filter : String, qos : UInt8) : Bool
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
        @subscriptions[filter] = qos
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

      private def get_packet(& : Protocol::Publish, UInt32 -> Nil) : Bool
        raise ClosedError.new if closed?
        loop do
          # The payload is sent straight from the segment, which a close or
          # delete of the session can unmap while the send is suspended
          msg_store.shift_with_lease?(@msg_store_lock) do |env|
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
          end || break
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
          @msg_store_lock.synchronize { msg_store.requeue(sp) }
          raise ex
        end
        delete_message(sp)
      end

      # False when no packet id was available, which leaves the message
      # requeued for the next attempt.
      private def deliver_acked(env, sp : SegmentPosition, & : Protocol::Publish, UInt32 -> Nil) : Bool
        id, original = packet_id_for(sp) || begin
          requeue_without_packet_id(sp)
          return false
        end
        # Raises before anything is booked, which the rescue below would not
        # roll back. Unreachable today, but being wrong loses the message.
        packet = begin
          build_packet(env, id, original)
        rescue ex
          @msg_store_lock.synchronize { msg_store.requeue(sp) }
          raise ex
        end
        begin
          # Booked before the send, which yields: the client can acknowledge
          # before we return, and an acknowledgement finding no entry is either
          # fatal (`puback`) or silently dropped (`pubrec`).
          @inflight[id] = Inflight.new(packet.qos == 1u8 ? Inflight::Awaiting::PubAck : Inflight::Awaiting::PubRec, sp)
          @unacked_count.add(1, :relaxed)
          @unacked_bytesize.add(sp.bytesize, :relaxed)
          # A re-send too: its id was remembered, but its record may have
          # failed or not be durable yet
          if packet.qos == 2u8 && @packet_id_log
            return true unless publish_sent_durable?(id, sp)
          end
          yield packet, sp.bytesize
          if env.redelivered
            @redeliver_count.add(1, :relaxed)
          else
            @deliver_count.add(1, :relaxed)
            @deliver_get_count.add(1, :relaxed)
          end
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

      # One drain per delivery, so a durable session's QoS 2 rate is bounded by
      # persister latency; recording a batch of ids per drain would lift that.
      # Records `id` as sent and waits for it to be durable. False when the
      # session closed meanwhile, `client=` requeued `sp` (the new connection
      # then sends it under this id), or the persister stopped.
      private def publish_sent_durable?(id : UInt16, sp : SegmentPosition) : Bool
        log_packet_id &.publish_sent(id, sp)
        durable = wait_until_durable
        unsend_on_stopped_persister(id, sp) unless durable || closed?
        durable && booked?(id, sp)
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

      # A message loaded from disk is not `redelivered`, so a re-send under the
      # original id is marked by `dup` [MQTT-3.3.1-1].
      def build_packet(env, packet_id, dup = false) : Protocol::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.["mqtt.retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        # `delivery_mode` is read off disk unvalidated and `Publish.new` raises
        # above QoS 2, which would make one bad byte a poison message.
        qos = 2u8 if qos > 2
        dup = qos.zero? ? false : (dup || env.redelivered)
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
          raise ProtocolViolation.new("PUBACK for unknown packet id '#{id}'")
        end
        sp = inflight.sp
        # A QoS 2 delivery is settled by PUBREC [MQTT-4.3.3-1], so a PUBACK for
        # one is a protocol violation. Checked before the delete, so it cannot
        # drop an obligation the session still owes.
        unless inflight.awaiting.pub_ack? && sp
          raise ProtocolViolation.new("PUBACK for packet id '#{id}', which is awaiting a QoS 2 acknowledgement")
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

      # The receiver owns the message from PUBREC on [MQTT-4.3.3-2], so it is
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
          raise ProtocolViolation.new("PUBREC for QoS #{inflight.qos} packet id '#{id}'")
        end
        sp = inflight.sp.as(SegmentPosition)
        # Before the send: a failed write still leaves the correct state, and
        # `client=` re-sends the PUBREL.
        @inflight[id] = Inflight.new(Inflight::Awaiting::PubComp, nil)
        @ack_count.add(1, :relaxed)
        @unacked_count.sub(1, :relaxed)
        @unacked_bytesize.sub(sp.bytesize, :relaxed)
        delete_message(sp)
        @msg_store_lock.synchronize { msg_store.mark_delete_dirty(sp) }
        send_pubrel(id)
        # No `refresh_capacity`: the id is still booked, so the window is
        # unchanged.
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
          raise ProtocolViolation.new("PUBCOMP for packet id '#{id}' that has not been PUBRECed")
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
        return if @inflight.size >= Config.instance.max_inflight_messages
        id = free_packet_id || return
        @packet_ids_exhausted = false
        @last_packet_id = id
      end

      private def free_packet_id : UInt16?
        next_id = @last_packet_id
        # Tries each of 1..65535 once, `@last_packet_id` itself last, as it can
        # be the only id left. Packet id 0 is illegal [MQTT-2.3.1-1].
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
        ready = message_count
        ready_bytes = @msg_store.try(&.bytesize) || 0u64
        {
          name:                         @name,
          durable:                      durable?,
          exclusive:                    false,
          auto_delete:                  @auto_delete,
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
