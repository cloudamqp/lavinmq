require "../logger"
require "../schema"
require "../event_type"
require "./consts"
require "./exchange"
require "./session"
require "./subscription_key"
require "./subscription_details"

module LavinMQ
  module MQTT
    # Owns a vhost's MQTT definitions: its sessions (one per client_id that has
    # subscribed) and their subscriptions, the latter held in the subscription
    # tree of the vhost's `MQTT::Exchange`. The exchange is created with the
    # store rather than declared, so there is always one to subscribe against,
    # and it is never persisted.
    #
    # Persisted to `definitions.mqtt` as an append-only log, compacted once
    # enough records are deletes. A clean session lives and dies with its
    # connection and never reaches the file. Records are keyed by session name
    # (`mqtt.<client_id>`), the identity the rest of the system uses.
    class DefinitionsStore
      Log = LavinMQ::Log.for "mqtt.definitions_store"

      FORMAT = IO::ByteFormat::SystemEndian

      enum Op : UInt8
        SessionAdd    = 1
        SessionDelete = 2
        Subscribe     = 3
        Unsubscribe   = 4
      end

      getter exchange : MQTT::Exchange

      @file : File

      def initialize(@vhost : VHost, @data_dir : String, @replicator : Clustering::Replicator?, @log : Logger)
        @sessions = Hash(String, Session).new
        @exchange = MQTT::Exchange.new(@vhost, EXCHANGE)
        # Reentrant: the store methods hold the lock while `store` may compact.
        @lock = Mutex.new(:reentrant)
        @file_path = File.join(@data_dir, "definitions.mqtt")
        # Unbuffered, as definitions.amqp: a joining follower reads size and
        # content through separate fds and can't see a buffered record.
        @file = File.open(@file_path, "a+").tap &.sync = true
        @replicator.try &.register_file(@file)
        @deletes = 0
        # Sessions read from our own file on boot, consulted while loading only.
        @loaded_sessions = Set(String).new
      end

      # Session accessors

      def session?(name : String) : Session?
        @sessions[name]?
      end

      def session(name : String) : Session
        @sessions[name]
      end

      def session_exists?(name : String) : Bool
        @sessions.has_key?(name)
      end

      def each_session(& : Session ->) : Nil
        @sessions.each_value { |s| yield s }
      end

      def sessions : Array(Session)
        @sessions.values
      end

      def sessions_size : Int32
        @sessions.size
      end

      def sessions_clear : Nil
        @sessions.clear
      end

      # Nil if a session already exists under that name, so a caller can tell a
      # fresh declaration from a no-op.
      def declare_session(name : String, clean_session : Bool,
                          loading = false, fsync = true) : Session?
        @lock.synchronize do
          return if @sessions.has_key?(name)
          session = @sessions[name] = Session.new(@vhost, name, clean_session)
          unless loading
            store(session_record(Op::SessionAdd, name), fsync: fsync) if session.durable?
            @vhost.apply_policies([session] of LavinMQ::Queue)
            @vhost.event_tick(EventType::QueueDeclared)
          end
          session
        end
      end

      # Removes a session and its subscriptions; nil if there is none. Closing
      # and deleting the `Session` is the caller's job, as it is for a queue.
      def delete_session(name : String) : Session?
        @lock.synchronize do
          return unless session = @sessions.delete(name)
          # Collected first: unsubscribing mutates the tree we'd be iterating.
          topic_filters = Array(String).new
          each_subscription do |s, topic_filter, _qos|
            topic_filters << topic_filter if s.same?(session)
          end
          topic_filters.each { |tf| @exchange.unsubscribe(session, tf) }
          # One record covers the subscriptions too; `load!` drops both.
          store(session_record(Op::SessionDelete, name), dirty: true) if session.durable?
          @vhost.event_tick(EventType::QueueDeleted)
          session
        end
      end

      # Subscription accessors

      # False if the session was deleted or replaced since the caller got hold
      # of it.
      def subscribe(session : Session, topic_filter : String, qos : UInt8,
                    loading = false, fsync = true) : Bool
        @lock.synchronize do
          return false unless current?(session)
          # definitions.mqtt holds the session's complete state and is newer
          # than any leftover frame in definitions.amqp, so replaying one would
          # revert what changed since the migration — a raised QoS, say. Unbind
          # frames never reach here, `DefinitionsStore#load!` resolves them away.
          return true if loading && @loaded_sessions.includes?(session.name)
          # Keyed on session and filter, so a repeat at another QoS overwrites
          # rather than duplicates — the Subscribe record does too, on load.
          @exchange.subscribe(session, topic_filter, qos)
          if session.durable? && !loading
            store(subscription_record(Op::Subscribe, session.name, topic_filter, qos), fsync: fsync)
          end
          true
        end
      end

      def unsubscribe(session : Session, topic_filter : String) : Bool
        @lock.synchronize do
          return false unless current?(session)
          @exchange.unsubscribe(session, topic_filter)
          if session.durable?
            store(subscription_record(Op::Unsubscribe, session.name, topic_filter, nil), dirty: true)
          end
          true
        end
      end

      def each_subscription(&block : (Session, String, UInt8) ->) : Nil
        @exchange.each_subscription(&block)
      end

      # One session's subscriptions, in the binding-details shape the HTTP API
      # and the session itself read them through.
      def subscriptions(session : Session) : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        each_subscription do |s, topic_filter, qos|
          next unless s.same?(session)
          result << SubscriptionDetails.new(@exchange.name, @vhost.name,
            SubscriptionKey.new(topic_filter, qos), s)
        end
        result
      end

      # Persistence

      def load! : Nil
        @lock.synchronize do
          if @file.size.zero?
            compact!
            return
          end
          @log.info { "Loading MQTT definitions" }
          SchemaVersion.verify(@file, :mqtt_definition)
          # Last record wins per key and a SessionDelete drops the session's
          # subscriptions too, so replay into hashes before building anything.
          sessions = Set(String).new
          subscriptions = Hash(String, Hash(String, UInt8)).new
          should_compact = false
          loop do
            break unless byte = @file.read_byte
            op = Op.from_value?(byte) ||
                 raise InvalidRecord.new("Unknown op #{byte} in #{@file_path}")
            case op
            in Op::SessionAdd
              sessions << read_string
            in Op::SessionDelete
              name = read_string
              sessions.delete(name)
              subscriptions.delete(name)
              should_compact = true
            in Op::Subscribe
              name = read_string
              topic_filter = read_string
              qos = @file.read_byte || raise IO::EOFError.new
              subscriptions.put_if_absent(name) { Hash(String, UInt8).new }[topic_filter] = qos
            in Op::Unsubscribe
              name = read_string
              topic_filter = read_string
              subscriptions[name]?.try &.delete(topic_filter)
              should_compact = true
            end
          rescue IO::EOFError
            break
          end

          # Only durable sessions are written, so everything read back is non-clean.
          sessions.each do |name|
            @sessions[name] = Session.new(@vhost, name, false)
            @loaded_sessions << name
          end
          subscriptions.each do |name, filters|
            next unless session = @sessions[name]?
            filters.each { |topic_filter, qos| @exchange.subscribe(session, topic_filter, qos) }
          end
          @log.info { "#{@sessions.size} MQTT sessions loaded" }
          compact! if should_compact
        end
      end

      # Makes sessions and subscriptions migrated out of definitions.amqp
      # durable here, before that file is rewritten without them.
      def rewrite! : Nil
        @lock.synchronize do
          compact!
          @file.fsync
          @replicator.try &.wait_for_followers
        end
      end

      # Flush records written with fsync: false, e.g. by a bulk import.
      def fsync : Nil
        @lock.synchronize do
          @file.fsync
          @replicator.try &.wait_for_followers
        end
      end

      def close : Nil
        @file.close
      end

      class InvalidRecord < LavinMQ::Error; end

      # Whether this is still the session registered under its name; a
      # clean-session client reconnecting under the same client_id replaces it.
      private def current?(session : Session) : Bool
        if current = @sessions[session.name]?
          current.same?(session)
        else
          false
        end
      end

      private def store(bytes : Bytes, dirty = false, fsync = true) : Nil
        offset = @file.size.to_i64
        # sync = true, so the record is readable at `offset` through any fd by
        # the time it is dispatched.
        @file.write bytes
        @replicator.try &.append_bytes @file_path, bytes, offset
        if fsync
          @file.fsync
          # A SubAck follows right after, so the change has to be durable on
          # every in-sync follower first.
          @replicator.try &.wait_for_followers
        end
        if dirty
          if (@deletes += 1) >= Config.instance.max_deleted_definitions
            compact!
            @deletes = 0
          end
        end
      end

      private def compact! : Nil
        @lock.synchronize do
          @log.info { "Compacting MQTT definitions" }
          # sync = true for the same reason as in #initialize: this becomes
          # @file after the rename.
          io = File.open("#{@file_path}.tmp", "a+").tap &.sync = true
          SchemaVersion.prefix(io, :mqtt_definition)
          @sessions.each_value do |session|
            next unless session.durable?
            io.write session_record(Op::SessionAdd, session.name)
          end
          each_subscription do |session, topic_filter, qos|
            next unless session.durable?
            io.write subscription_record(Op::Subscribe, session.name, topic_filter, qos)
          end
          io.fsync
          File.rename io.path, @file_path
          @replicator.try &.replace_file @file_path
          @file.close
          @file = io
        end
      end

      private def session_record(op : Op, name : String) : Bytes
        io = IO::Memory.new(3 + name.bytesize)
        io.write_byte op.value
        write_string(io, name)
        io.to_slice
      end

      private def subscription_record(op : Op, name : String, topic_filter : String,
                                      qos : UInt8?) : Bytes
        io = IO::Memory.new(6 + name.bytesize + topic_filter.bytesize)
        io.write_byte op.value
        write_string(io, name)
        write_string(io, topic_filter)
        io.write_byte qos if qos
        io.to_slice
      end

      # u16 lengths, as in MQTT's own framing, and wider than the shortstr these
      # names used to be persisted as.
      private def write_string(io : ::IO, str : String) : Nil
        io.write_bytes(str.bytesize.to_u16, FORMAT)
        io.write(str.to_slice)
      end

      private def read_string : String
        len = UInt16.from_io(@file, FORMAT)
        bytes = Bytes.new(len)
        @file.read_fully(bytes)
        String.new(bytes)
      end
    end
  end
end
