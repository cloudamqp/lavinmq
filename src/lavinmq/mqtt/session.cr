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

module LavinMQ
  module MQTT
    class Session
      class ClosedError < MQTT::Error; end

      include SortableJSON
      include PolicyTarget
      include AMQP::QueueStats
      include MQTT::Subscriber
      Log = ::LavinMQ::Log.for "mqtt.session"

      ARGUMENTS      = AMQP::Table.new({"x-queue-type" => "mqtt"})
      EFFECTIVE_ARGS = {"x-queue-type"}

      getter name : String
      getter vhost : VHost
      getter? internal = false
      getter? auto_delete

      @max_length : Int64? = nil
      @max_length_bytes : Int64? = nil
      @msg_store_lock = Mutex.new(:reentrant)
      @msg_store : SessionMessageStore
      @metadata : ::Log::Metadata
      @closed = Atomic(Bool).new(false)
      @deleted = Atomic(Bool).new(false)
      # Closed once a delete has removed the session from the vhost
      @deletion_done = ::Channel(Nil).new
      @client : MQTT::Client? = nil
      @client_lock = Mutex.new
      @permission_service : PermissionService
      # Derived from the queue name, so a restored session with no client
      # attached still knows its client id.
      @client_id : String
      # Carries the user of the last attached client. The username is kept in
      # the .metadata file so a restored session keeps its member rules until a
      # client reconnects.
      @permission_context : PermissionService::Context
      @metadata_file : String
      # Written while an operator has paused the session, and kept over a
      # restart of the broker, like a queue's
      @paused_file : String
      @replicator : Clustering::Replicator?
      @has_client = BoolChannel.new(false)
      # True while the deliveries are paused: by an operator, see #pause!, or by
      # a store error, see #pause_on_store_error. Set under @msg_store_lock.
      @paused = BoolChannel.new(false)
      @has_capacity = BoolChannel.new(true)
      # This session's topic filters and their QoS, kept by the MQTT exchange
      # as it binds and unbinds them, so finding a session's subscriptions
      # doesn't walk every subscription in the vhost
      @subscriptions = Hash(String, UInt8).new

      protected def initialize(@vhost : VHost,
                               @name : String,
                               @auto_delete = false,
                               arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        @count = 0u16
        @client_id = @name.lchop(SESSION_PREFIX)
        @permission_service = @vhost.mqtt_permission_service
        @unacked = Hash(UInt16, SegmentPosition).new

        @metadata = ::Log::Metadata.new(nil, {queue: @name, vhost: @vhost.name})
        @log = Logger.new(Log, @metadata)
        data_dir = File.join(
          durable? ? @vhost.data_dir : File.join(@vhost.data_dir, "transient"),
          Digest::SHA1.hexdigest(@name)
        )
        created = durable? && !Dir.exists?(data_dir)
        Dir.mkdir_p(data_dir)
        # Like a queue's, the directory's entry is fsynced with the next
        # confirm, see Queue#make_data_dir
        @vhost.persister.mark_dirty(@vhost.data_dir) if created
        @replicator = durable? ? @vhost.@replicator : nil
        @msg_store = SessionMessageStore.new(data_dir, @replicator, durable?, metadata: @metadata, persister: @vhost.persister)
        if @msg_store.failed?
          @log.error { "Failed, message data can't be loaded. Delete the session to remove it, or fail over to a node with a good copy" }
          @paused.set(true)
        end
        @metadata_file = File.join(data_dir, ".metadata")
        @paused_file = File.join(data_dir, "paused")
        if File.exists?(@paused_file)
          @paused.set(true)
          @replicator.try &.register_file(@paused_file)
        end
        username = nil
        if File.exists?(@metadata_file)
          @replicator.try &.register_file(@metadata_file)
          username = read_metadata_file
        else
          write_metadata_file(nil)
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
        ARGUMENTS
      end

      def close : Bool
        return false if @closed.swap(true)
        @has_capacity.close
        @has_client.close
        @paused.close
        @msg_store_lock.synchronize do
          @msg_store.close
        end
        true
      end

      def delete : Bool
        return false if @deleted.swap(true)
        begin
          close
          @msg_store_lock.synchronize do
            @msg_store.delete
          end
          @replicator.try do |r|
            r.delete_file(@metadata_file)
            # Under the lock, so a concurrent #pause! can't replicate the file
            # after this delete. Removed locally with the store's directory.
            @msg_store_lock.synchronize { r.delete_file(@paused_file) }
          end
          @vhost.delete_queue(@name)
        ensure
          @deletion_done.close
        end
        true
      end

      def deleting? : Bool
        @deleted.get
      end

      # Returns once a delete in progress has removed the session from the vhost
      def wait_for_deletion : Nil
        @deletion_done.receive?
      end

      def clean_session?
        @auto_delete
      end

      private def deliver_loop
        delivered_bytes = 0_i32
        loop do
          break if closed?
          client = @client
          if @msg_store.empty?
            client.try &.flush
            next @msg_store.empty.when_false.receive?
          end
          next @has_client.when_true.receive? if client.nil?
          unless @has_capacity.value
            client.flush
            next @has_capacity.when_true.receive?
          end
          if @paused.value
            client.flush
            next @paused.when_false.receive?
          end
          get_packet do |pub_packet, bytesize|
            client.send(pub_packet, flush: false)
            delivered_bytes &+= bytesize
          end
          if delivered_bytes > Config.instance.yield_each_delivered_bytes
            delivered_bytes = 0
            client.flush
            Fiber.yield
          end
        rescue ex : MessageStore::Error
          # The client stays connected: it would only reconnect to the same store
          pause_on_store_error(ex)
        rescue ex
          @log.error(exception: ex) { "Failed to deliver message in deliver_loop" }
          if client
            client.close("Server force closed client")
            detach(client)
          end
        end
      end

      # Nothing more is delivered when the message store fails. Publishes are
      # still stored, unless the store has data it can't read, see #publish.
      # #resume! tries again, unless the store has data it can't read: then the
      # session has to be deleted. Like a queue's, this pause isn't kept over a
      # restart of the broker.
      private def pause_on_store_error(ex : MessageStore::Error) : Nil
        paused = @msg_store_lock.synchronize do
          next false if closed?
          @paused.set(true)
          true
        end
        if paused
          @log.error(exception: ex) do
            if @msg_store.failed?
              "Failed, message data can't be read. Delete the session to remove it, or fail over to a node with a good copy"
            else
              "Paused due to a message store error, resume the session to try again"
            end
          end
        else
          @log.debug(exception: ex) { "Message store error while paused" }
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

      def client : MQTT::Client?
        @client
      end

      def client=(client : MQTT::Client?)
        @client_lock.synchronize { attach(client) }
      end

      # Detaches *client*, unless another connection has replaced it
      def detach(client : MQTT::Client) : Nil
        @client_lock.synchronize do
          attach(nil) if @client.same?(client)
        end
      end

      private def attach(client : MQTT::Client?)
        return if closed?
        @last_get_time = RoughTime.instant

        # A clean session carries nothing between connections [MQTT-3.1.2-6]. A
        # persistent one requeues what it owes and remembers the packet ids, to
        # resend under the ids the client already knows [MQTT-4.4.0-1].
        unless clean_session?
          @msg_store_lock.synchronize do
            @unacked.each do |packet_id, sp|
              @msg_store.remember_packet_id(sp, packet_id)
              @msg_store.requeue(sp)
            end
          end
        end

        @unacked.clear
        @unacked_count.set(0, :release)
        @unacked_bytesize.set(0, :release)
        refresh_capacity

        @client = client
        @has_client.set(!client.nil?)
        if client && (username = client.user.name) != @permission_context.username
          @permission_context = PermissionService::Context.new(username, @client_id)
          write_metadata_file(username)
        end

        @log.debug { "client set to '#{client.try &.name}'" }
      end

      def durable?
        !clean_session?
      end

      # The .metadata file is to a session what .queue is to a queue: it names
      # the owner of a data directory. It also holds the last attached username.
      # Anything that is not a JSON object with a string username is treated
      # as an unknown user; a bad file must never stop the session from loading.
      private def read_metadata_file : String?
        JSON.parse(File.read(@metadata_file)).as_h?.try(&.["username"]?).try(&.as_s?)
      rescue ex : JSON::ParseException | IO::Error
        @log.warn(exception: ex) { "Could not read #{@metadata_file}, session user unknown until a client connects" }
        nil
      end

      # Written to a temporary file and renamed into place, so it's never read
      # half written. A crash before the next confirm can lose it, which leaves
      # the user unknown until a client connects, as with an unreadable file.
      private def write_metadata_file(username : String?) : Nil
        tmp = "#{@metadata_file}.tmp"
        begin
          File.write(tmp, {name: @name, client_id: @client_id, username: username}.to_json)
          File.rename(tmp, @metadata_file)
        ensure
          File.delete?(tmp)
        end
        return unless durable?
        @replicator.try &.replace_file(@metadata_file)
        @vhost.persister.mark_dirty(@metadata_file)
        @vhost.persister.mark_dirty(File.dirname(@metadata_file))
      end

      # A concurrent API deletion can make the bind fail.
      # TODO: Check session identity under the definitions lock before binding.
      # @subscriptions is only written under the definitions lock, by the MQTT
      # exchange, so the lookup and the bind it decides on happen under it too
      def subscribe(tf, qos) : Bool
        @vhost.synchronize_definitions do
          if current = @subscriptions[tf]?
            next true if current == qos
            unbind(tf, MQTT.qos_arguments(current))
          end
          @vhost.bind_queue(@name, EXCHANGE, tf, MQTT.qos_arguments(qos))
        end
      end

      def unsubscribe(tf)
        @vhost.synchronize_definitions do
          if qos = @subscriptions[tf]?
            unbind(tf, MQTT.qos_arguments(qos))
          end
        end
      end

      # Not for binding or unbinding from the block: the MQTT exchange changes
      # the hash as it does
      def each_subscription(& : String, UInt8 ->) : Nil
        @subscriptions.each { |filter, qos| yield filter, qos }
      end

      # Called by the MQTT exchange for every binding, whether from a
      # SUBSCRIBE, the HTTP API, a definitions import or a restore at startup
      protected def subscription_bound(filter : String, qos : UInt8) : Nil
        @subscriptions[filter] = qos
      end

      protected def subscription_unbound(filter : String) : Nil
        @subscriptions.delete(filter)
      end

      protected def clear_subscriptions : Nil
        @subscriptions.clear
      end

      def deliver(msg : Message, filter : String, ctx : PublishContext) : Bool
        publish(msg)
      end

      # Returns whether the message was accepted, so the exchange only counts
      # deliveries that happened.
      def publish(msg : Message) : Bool
        unless @permission_service.can_read?(@permission_context, msg.routing_key)
          @log.debug { "Message refused: no topic permission rule allows user '#{@permission_context.username}' to read topic '#{msg.routing_key}'" }
          return false
        end
        return true if msg.properties.delivery_mode == 0 && @client.nil?
        return false if deleting? || closed?
        @msg_store_lock.synchronize do
          # Again under the lock: the fiber that held it can close the session
          # before this one runs, and raising here would fail the whole fan-out
          return false if closed?
          # It would be lost with the rest when the session is deleted
          return false if @msg_store.failed?
          @msg_store.push(msg)
          drop_overflow
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
          @msg_store.shift_with_lease?(@msg_store_lock) do |env|
            sp = env.segment_position
            no_ack = env.message.properties.delivery_mode == 0
            if no_ack
              begin
                packet = build_packet(env, nil)
                yield packet, sp.bytesize
                if env.redelivered
                  @redeliver_count.add(1, :relaxed)
                else
                  @deliver_no_ack_count.add(1, :relaxed)
                end
              rescue ex # requeue failed delivery
                @msg_store_lock.synchronize { @msg_store.requeue(sp) }
                raise ex
              end
              delete_message(sp)
            else
              booked = false
              begin
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
                packet = build_packet(env, id)
                # Booked before the send, which yields: the client can ack
                # before it returns, and an ack that finds no entry closes the
                # connection.
                @unacked[id] = sp
                booked = true
                @unacked_count.add(1, :relaxed)
                @unacked_bytesize.add(sp.bytesize, :relaxed)
                yield packet, sp.bytesize
                if env.redelivered
                  @redeliver_count.add(1, :relaxed)
                else
                  @deliver_count.add(1, :relaxed)
                end
                @msg_store.forget_packet_id(sp)
                refresh_capacity
              rescue ex # requeue failed delivery
                # Nothing to unbook if the client already acked it: the message
                # is deleted and the counters are settled.
                if !booked
                  @msg_store_lock.synchronize { @msg_store.requeue(sp) }
                elsif @unacked.delete(id)
                  @msg_store_lock.synchronize { @msg_store.requeue(sp) }
                  @unacked_count.sub(1, :relaxed)
                  @unacked_bytesize.sub(sp.bytesize, :relaxed)
                end
                raise ex
              end
            end
            return true
          end || break
        end
        false
      end

      def build_packet(env, packet_id) : Protocol::Publish
        msg = env.message
        retained = msg.properties.try &.headers.try &.["mqtt.retain"]? == true
        qos = msg.properties.delivery_mode || 0u8
        qos = 1u8 if qos > 1
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
        if sp = @unacked.delete(id)
          begin
            @ack_count.add(1, :relaxed)
            @unacked_count.sub(1, :relaxed)
            @unacked_bytesize.sub(sp.bytesize, :relaxed)
            delete_message(sp)
          rescue ex : MessageStore::Error
            # The client isn't disconnected for it. The message stays in its
            # segment, so it can come back after a restart of the broker.
            @log.error(exception: ex) { "Failed to delete #{sp}" }
            pause_on_store_error(ex)
          rescue ex
            raise ::IO::Error.new("Could not acknowledge packet with id '#{id}'", ex)
          ensure
            refresh_capacity
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
        return if @msg_store.failed? # nothing can be read
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
      rescue ex : MessageStore::Error
        # The publish is stored, the limit is enforced again once resumed
        pause_on_store_error(ex)
      end

      private def clear_policy_arguments
        @max_length = nil
        @max_length_bytes = nil
      end

      private def handle_arguments
      end

      # Kept over a restart of the broker, unlike a pause by a store error, so
      # the file is written also when the session is paused already
      def pause! : Nil
        @msg_store_lock.synchronize do
          return if closed?
          # Not empty: a follower applies a replaced file of zero bytes as a delete
          File.write(@paused_file, @name)
          @replicator.try &.replace_file(@paused_file)
          @paused.set(true)
        end
        @log.debug { "Paused" }
      end

      # Ends a pause by an operator, and one by a store error, which tries the
      # store again. A session with data it can't read stays failed until it's
      # deleted.
      def resume! : Nil
        resumed = @msg_store_lock.synchronize do
          next false unless @paused.value
          if File.delete?(@paused_file)
            @replicator.try &.delete_file(@paused_file)
          end
          if @msg_store.failed?
            @log.error { "Still failed, message data can't be read. Delete the session to remove it, or fail over to a node with a good copy" }
            next false
          end
          @paused.set(false)
          true
        end
        @log.info { "Resumed" } if resumed
        drop_overflow if resumed
      end

      def state : QueueState
        return QueueState::Closed if closed?
        return QueueState::Failed if @msg_store.failed? # until it's deleted
        @paused.value ? QueueState::Paused : QueueState::Running
      end

      # Why the session is paused, like Queue#paused_reason
      def paused_reason : String?
        return if closed? || !@paused.value || @msg_store.failed?
        File.exists?(@paused_file) ? "operator" : "store_error"
      end

      def state_match?(states : Array(QueueState)) : Bool
        states.includes?(state)
      end

      def purge(max_count : Int = UInt32::MAX) : UInt32
        count = @msg_store_lock.synchronize do
          # What can be read can't be told from what can't, so the session has
          # to be deleted
          raise_unreadable if @msg_store.failed?
          purged = @msg_store.purge(max_count)
          raise_unreadable if @msg_store.failed?
          purged
        end
        @log.info { "Purged #{count} messages" }
        count
      rescue ex : MessageStore::Error
        pause_on_store_error(ex)
        # The purge reached data that can't be read
        raise_unreadable if @msg_store.failed?
        raise ex
      end

      # Pauses the session, if the purge was the first to reach the data
      private def raise_unreadable : NoReturn
        @msg_store_lock.synchronize { @paused.set(true) unless closed? }
        raise LavinMQ::Error::PreconditionFailed.new("Message data in session '#{@name}' can't be read. Delete the session to remove it, or fail over to a node with a good copy")
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
            next if k == :message_stats
            json.field(k, v) unless v.nil?
          end
          json.field("message_stats") do
            json.object do
              stats_details.each do |k, v|
                json.field(k, v) unless v.nil?
              end
            end
          end
          json.field("messages_ready_details", {log: message_count_log})
          json.field("messages_unacknowledged_details", {log: unacked_count_log})
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
          paused_reason:                paused_reason,
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
