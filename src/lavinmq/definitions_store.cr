require "json"
require "./logger"
require "./schema"
require "./event_type"
require "./queue_factory"
require "./rough_time"
require "./amqp/exchange/*"
require "./amqp/queue"

module LavinMQ
  class DefinitionsStore
    Log              = LavinMQ::Log.for "definitions_store"
    WAL_COMPACT_IDLE = 30.seconds
    WAL_COMPACT_SIZE = 8_i64 * 1024 * 1024

    @definitions_file : File
    @closed = Atomic(Bool).new(false)

    def initialize(@vhost : VHost, @data_dir : String, @replicator : Clustering::Replicator?, @log : Logger)
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @definitions_lock = Mutex.new(:reentrant)
      @legacy_definitions_file_path = File.join(@data_dir, "definitions.amqp")
      @definitions_file_path = File.join(@data_dir, "definitions.wal")
      @exchanges_file_path = File.join(@data_dir, "exchanges.json")
      @queues_file_path = File.join(@data_dir, "queues.json")
      @bindings_file_path = File.join(@data_dir, "bindings.json")
      # Buffered writes are flushed before replication so the WAL record is
      # visible through separate fds used by follower full sync.
      @definitions_file = File.open(@definitions_file_path, "a+")
      @replicator.try &.register_file(@definitions_file)
      @dirty_exchanges = false
      @dirty_queues = false
      @dirty_bindings = false
      @last_definition_change = RoughTime.instant
      @compact_requested = Channel(Nil).new(1)
      @compact_loop = WaitGroup.new
      @compact_loop.spawn name: "DefinitionsStore#compact_loop #{@vhost.name}" do
        compact_loop
      end
    end

    # Flush buffered definition writes to disk. Used after a bulk operation
    # (e.g. import) that stored its definitions with fsync: false; the whole
    # batch is acknowledged after this, so it waits for follower acks like
    # the per-frame path does.
    def fsync
      @definitions_lock.synchronize do
        @definitions_file.flush
        @definitions_file.fsync
        @replicator.try &.wait_for_followers
      end
    end

    # Exchange accessors

    def exchange?(name : String) : Exchange?
      @exchanges[name]?
    end

    def exchange(name : String) : Exchange
      @exchanges[name]
    end

    def exchange_exists?(name : String) : Bool
      @exchanges.has_key?(name)
    end

    def each_exchange(& : Exchange ->) : Nil
      @exchanges.each_value { |v| yield v }
    end

    def exchanges : Array(Exchange)
      @exchanges.values
    end

    def exchanges_size : Int32
      @exchanges.size
    end

    def exchanges_any?(& : {String, Exchange} -> Bool) : Bool
      @exchanges.any? { |kv| yield kv }
    end

    # Insert a pre-built exchange (e.g. MQTT or other internal types that the
    # frame-driven `apply` path can't construct). Locked; idempotent so callers
    # like `init_delayed_queue` can re-register on exchange re-creation. Skips
    # persistence and event ticks since these aren't replayed from frames.
    def register_exchange(exchange : Exchange) : Nil
      @definitions_lock.synchronize do
        @exchanges[exchange.name] = exchange
      end
    end

    # Queue accessors

    def queue?(name : String) : Queue?
      @queues[name]?
    end

    def queue(name : String) : Queue
      @queues[name]
    end

    def queue_exists?(name : String) : Bool
      @queues.has_key?(name)
    end

    def each_queue(& : Queue ->) : Nil
      @queues.each_value { |v| yield v }
    end

    def queues : Array(Queue)
      @queues.values
    end

    def queues_size : Int32
      @queues.size
    end

    # Insert a pre-built queue (e.g. DelayedExchangeQueue) that the
    # frame-driven `apply` path can't construct. Locked; idempotent so it can be
    # called when an exchange is re-imported and a delayed queue with the same
    # name already exists. Skips persistence and event ticks since these aren't
    # replayed from frames.
    def register_queue(queue : Queue) : Nil
      @definitions_lock.synchronize do
        @queues[queue.name] = queue
      end
    end

    def queues_clear : Nil
      @queues.clear
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def apply(f, loading = false, fsync = true) : Bool
      @definitions_lock.synchronize do
        case f
        when AMQP::Frame::Exchange::Declare
          return false if @exchanges.has_key? f.exchange_name
          e = @exchanges[f.exchange_name] =
            make_exchange(@vhost, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
          @vhost.apply_policies([e] of Exchange) unless loading
          store_definition(f, fsync: fsync) if !loading && f.durable
        when AMQP::Frame::Exchange::Delete
          if x = @exchanges.delete f.exchange_name
            unless @vhost.closed?
              @exchanges.each_value do |ex|
                ex.bindings_details.each do |binding|
                  next unless binding.destination == x
                  ex.unbind(x, binding.routing_key, binding.arguments)
                end
              end
            end
            x.delete
            store_definition(f) if !loading && x.durable?
          else
            return false
          end
        when AMQP::Frame::Exchange::Bind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.bind(dst, f.routing_key, f.arguments)
          store_definition(f, fsync: fsync) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Exchange::Unbind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.unbind(dst, f.routing_key, f.arguments)
          store_definition(f) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Queue::Declare
          return false if @queues.has_key? f.queue_name
          q = @queues[f.queue_name] = QueueFactory.make(@vhost, f)
          @vhost.apply_policies([q] of Queue) unless loading
          store_definition(f, fsync: fsync) if !loading && f.durable && !f.exclusive
          @vhost.event_tick(EventType::QueueDeclared) unless loading
        when AMQP::Frame::Queue::Delete
          if q = @queues.delete(f.queue_name)
            unless @vhost.closed?
              @exchanges.each_value do |ex|
                ex.bindings_details.each do |binding|
                  next unless binding.destination == q
                  ex.unbind(q, binding.routing_key, binding.arguments)
                end
              end
            end
            store_definition(f) if !loading && q.durable? && !q.exclusive?
            @vhost.event_tick(EventType::QueueDeleted) unless loading
            q.delete
          else
            return false
          end
        when AMQP::Frame::Queue::Bind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.bind(q, f.routing_key, f.arguments)
          store_definition(f, fsync: fsync) if !loading && x.durable? && q.durable? && !q.exclusive?
        when AMQP::Frame::Queue::Unbind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.unbind(q, f.routing_key, f.arguments)
          store_definition(f) if !loading && x.durable? && q.durable? && !q.exclusive?
        else raise "Cannot apply frame #{f.class} in vhost #{@vhost.name}"
        end
        true
      end
    end

    def queue_bindings(queue : Queue) : Array(BindingDetails)
      default_binding = BindingDetails.new("", @vhost.name, BindingKey.new(queue.name), queue)
      bindings = @exchanges.values.flat_map do |ex|
        ex.bindings_details.select { |binding| binding.destination == queue }
      end
      [default_binding] + bindings
    end

    def load!
      @definitions_lock.synchronize do
        if snapshot_definitions?
          load_snapshot_definitions
        elsif legacy_definitions?
          load_legacy_definitions
          compact_locked(all: true)
          delete_legacy_definitions
        else
          load_default_definitions
          compact_locked(all: true)
        end
      end
      request_compaction
    end

    def close : Nil
      return if @closed.swap(true)

      @compact_requested.close
      @compact_loop.wait
      # Take the lock so the close can't race a writer still inside
      # store_definition/fsync (use-after-close on the fd).
      @definitions_lock.synchronize do
        compact_locked(all: true) unless @definitions_file.size.zero?
        @definitions_file.close
      end
    end

    private def delete_legacy_definitions
      File.delete?(@legacy_definitions_file_path)
      @replicator.try &.delete_file(@legacy_definitions_file_path)
    end

    private def snapshot_definitions? : Bool
      File.exists?(@exchanges_file_path) ||
        File.exists?(@queues_file_path) ||
        File.exists?(@bindings_file_path)
    end

    private def legacy_definitions? : Bool
      File.exists?(@legacy_definitions_file_path) && File.size(@legacy_definitions_file_path) > 0
    end

    private def wal_dirty? : Bool
      @dirty_exchanges || @dirty_queues || @dirty_bindings
    end

    private def load_snapshot_definitions
      exchanges = Hash(String, AMQP::Frame::Exchange::Declare).new
      queues = Hash(String, AMQP::Frame::Queue::Declare).new
      queue_bindings = Hash(String, Array(AMQP::Frame::Queue::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Queue::Bind).new }
      exchange_bindings = Hash(String, Array(AMQP::Frame::Exchange::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Exchange::Bind).new }

      @log.info { "Loading definition snapshots" }
      if File.exists?(@exchanges_file_path)
        load_json_array(@exchanges_file_path) { |entry| exchanges[entry["name"].as_s] = exchange_declare_from_json(entry) }
      else
        default_exchange_frames.each { |frame| exchanges[frame.exchange_name] = frame }
      end
      load_json_array(@queues_file_path) { |entry| queues[entry["name"].as_s] = queue_declare_from_json(entry) } if File.exists?(@queues_file_path)
      load_json_array(@bindings_file_path) { |entry| add_binding_to_maps(binding_from_json(entry), queue_bindings, exchange_bindings) } if File.exists?(@bindings_file_path)
      replay_wal(exchanges, queues, queue_bindings, exchange_bindings)
      apply_definition_maps(exchanges, queues, queue_bindings, exchange_bindings)
    end

    private def load_legacy_definitions
      exchanges = Hash(String, AMQP::Frame::Exchange::Declare).new
      queues = Hash(String, AMQP::Frame::Queue::Declare).new
      queue_bindings = Hash(String, Array(AMQP::Frame::Queue::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Queue::Bind).new }
      exchange_bindings = Hash(String, Array(AMQP::Frame::Exchange::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Exchange::Bind).new }

      @log.info { "Loading legacy definitions" }
      File.open(@legacy_definitions_file_path) do |io|
        SchemaVersion.verify(io, :definition)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        loop do
          frame = stream.next_frame
          @log.trace { "Reading legacy frame #{frame.inspect}" }
          apply_to_definition_maps(frame, exchanges, queues, queue_bindings, exchange_bindings)
        rescue ex : IO::EOFError
          break
        end
      end
      apply_definition_maps(exchanges, queues, queue_bindings, exchange_bindings)
    end

    private def replay_wal(exchanges, queues, queue_bindings, exchange_bindings)
      return if @definitions_file.size.zero?

      @log.info { "Replaying definition WAL" }
      @definitions_file.rewind
      @definitions_file.each_line do |line|
        line = line.strip
        next if line.empty?
        begin
          frame = frame_from_wal_record(JSON.parse(line))
        rescue ex : JSON::ParseException | TypeCastError | KeyError
          # A crash mid-append leaves a torn final record; a corrupt line can
          # appear from bit-rot or a partial non-tail write. The legacy binary
          # reader tolerated a truncated tail via IO::EOFError, so skip the bad
          # record rather than aborting the whole vhost load — the next
          # compaction rewrites the snapshots and clears the WAL.
          @log.warn { "Skipping unreadable definition WAL record: #{ex.message}" }
          next
        end
        apply_to_definition_maps(frame, exchanges, queues, queue_bindings, exchange_bindings)
        mark_snapshot_dirty(frame)
      end
      @definitions_file.seek(0, IO::Seek::End)
    end

    private def apply_definition_maps(exchanges, queues, queue_bindings, exchange_bindings)
      @log.info { "Applying #{exchanges.size} exchanges" }
      exchanges.each_value { |frame| load_apply(frame) }
      @log.info { "Applying #{queues.size} queues" }
      queues.each_value { |frame| load_apply(frame) }
      @log.info { "Applying #{exchange_bindings.each_value.sum(0, &.size)} exchange bindings" }
      exchange_bindings.each_value { |frames| frames.each { |frame| load_apply(frame) } }
      @log.info { "Applying #{queue_bindings.each_value.sum(0, &.size)} queue bindings" }
      queue_bindings.each_value { |frames| frames.each { |frame| load_apply(frame) } }
      @log.info { "Definitions loaded" }
    end

    private def load_json_array(path : String, &)
      File.open(path) do |file|
        JSON.parse(file).as_a.each { |entry| yield entry }
        @replicator.try &.register_file(file)
      end
    end

    private def default_exchange_frames
      [
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "", "direct", false, true, false, false, false, AMQP::Table.new),
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "amq.direct", "direct", false, true, false, false, false, AMQP::Table.new),
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "amq.fanout", "fanout", false, true, false, false, false, AMQP::Table.new),
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "amq.topic", "topic", false, true, false, false, false, AMQP::Table.new),
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "amq.headers", "headers", false, true, false, false, false, AMQP::Table.new),
        AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, "amq.match", "headers", false, true, false, false, false, AMQP::Table.new),
      ]
    end

    private def exchange_declare_from_json(entry : JSON::Any) : AMQP::Frame::Exchange::Declare
      AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, entry["name"].as_s, entry["type"].as_s,
        false, json_bool(entry, "durable", default: true), json_bool(entry, "auto_delete"), json_bool(entry, "internal"),
        false, arguments_from_json(entry))
    end

    private def queue_declare_from_json(entry : JSON::Any) : AMQP::Frame::Queue::Declare
      AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, entry["name"].as_s, false,
        json_bool(entry, "durable", default: true), json_bool(entry, "exclusive"),
        json_bool(entry, "auto_delete"), false, arguments_from_json(entry))
    end

    private def binding_from_json(entry : JSON::Any) : AMQP::Frame
      args = arguments_from_json(entry)
      destination_type = entry["destination_type"]?.try(&.as_s) || "queue"
      routing_key = entry["routing_key"]?.try(&.as_s) || ""
      case destination_type
      when "queue"
        AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          routing_key, false, args)
      when "exchange"
        AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          routing_key, false, args)
      else
        raise "Unknown binding destination type #{destination_type} in vhost #{@vhost.name}"
      end
    end

    private def frame_from_wal_record(entry : JSON::Any) : AMQP::Frame
      case op = entry["op"].as_s
      when "exchange.declare" then exchange_declare_from_json(entry)
      when "exchange.delete"  then AMQP::Frame::Exchange::Delete.new(0_u16, 0_u16, entry["name"].as_s, false, false)
      when "exchange.bind"
        AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "exchange.unbind"
        AMQP::Frame::Exchange::Unbind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "queue.declare" then queue_declare_from_json(entry)
      when "queue.delete"  then AMQP::Frame::Queue::Delete.new(0_u16, 0_u16, entry["name"].as_s, false, false, false)
      when "queue.bind"
        AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, entry["queue"].as_s, entry["exchange"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "queue.unbind"
        AMQP::Frame::Queue::Unbind.new(0_u16, 0_u16, entry["queue"].as_s, entry["exchange"].as_s,
          entry["routing_key"].as_s, arguments_from_json(entry))
      else
        raise "Unknown definition WAL operation #{op} in vhost #{@vhost.name}"
      end
    end

    private def arguments_from_json(entry : JSON::Any) : AMQP::Table
      if args = entry["arguments"]?
        if hash = args.as_h?
          return AMQP::Table.new(hash)
        end
      end
      AMQP::Table.new
    end

    private def json_bool(entry : JSON::Any, field : String, default = false) : Bool
      if value = entry[field]?
        value.as_bool
      else
        default
      end
    end

    private def apply_to_definition_maps(frame, exchanges, queues, queue_bindings, exchange_bindings)
      case frame
      when AMQP::Frame::Exchange::Declare
        exchanges[frame.exchange_name] = frame
      when AMQP::Frame::Exchange::Delete
        exchanges.delete frame.exchange_name
        remove_exchange_bindings(frame.exchange_name, queue_bindings, exchange_bindings)
      when AMQP::Frame::Exchange::Bind
        add_binding_to_maps(frame, queue_bindings, exchange_bindings)
      when AMQP::Frame::Exchange::Unbind
        exchange_bindings[frame.destination].reject! { |b| exchange_binding_matches?(b, frame) }
      when AMQP::Frame::Queue::Declare
        queues[frame.queue_name] = frame
      when AMQP::Frame::Queue::Delete
        queues.delete frame.queue_name
        queue_bindings.delete frame.queue_name
      when AMQP::Frame::Queue::Bind
        add_binding_to_maps(frame, queue_bindings, exchange_bindings)
      when AMQP::Frame::Queue::Unbind
        queue_bindings[frame.queue_name].reject! { |b| queue_binding_matches?(b, frame) }
      else
        raise "Cannot load frame #{frame.class} in vhost #{@vhost.name}"
      end
    end

    private def add_binding_to_maps(frame : AMQP::Frame::Queue::Bind, queue_bindings, _exchange_bindings)
      bindings = queue_bindings[frame.queue_name]
      bindings << frame unless bindings.any? { |b| queue_binding_matches?(b, frame) }
    end

    private def add_binding_to_maps(frame : AMQP::Frame::Exchange::Bind, _queue_bindings, exchange_bindings)
      bindings = exchange_bindings[frame.destination]
      bindings << frame unless bindings.any? { |b| exchange_binding_matches?(b, frame) }
    end

    private def add_binding_to_maps(frame, queue_bindings, exchange_bindings)
      case frame
      when AMQP::Frame::Queue::Bind    then add_binding_to_maps(frame, queue_bindings, exchange_bindings)
      when AMQP::Frame::Exchange::Bind then add_binding_to_maps(frame, queue_bindings, exchange_bindings)
      else                                  raise "Cannot add frame #{frame.class} as a binding in vhost #{@vhost.name}"
      end
    end

    private def queue_binding_matches?(a : AMQP::Frame::Queue::Bind, b : AMQP::Frame::Queue::Bind | AMQP::Frame::Queue::Unbind) : Bool
      a.exchange_name == b.exchange_name &&
        a.routing_key == b.routing_key &&
        a.arguments == b.arguments
    end

    private def exchange_binding_matches?(a : AMQP::Frame::Exchange::Bind, b : AMQP::Frame::Exchange::Bind | AMQP::Frame::Exchange::Unbind) : Bool
      a.source == b.source &&
        a.routing_key == b.routing_key &&
        a.arguments == b.arguments
    end

    private def remove_exchange_bindings(name : String, queue_bindings, exchange_bindings)
      exchange_bindings.delete name
      exchange_bindings.each_value &.reject! { |b| b.source == name }
      queue_bindings.each_value &.reject! { |b| b.exchange_name == name }
    end

    protected def load_apply(frame : AMQP::Frame)
      apply frame, loading: true
    rescue ex : LavinMQ::Error
      @log.error(exception: ex) { "Failed to apply frame #{frame.inspect}" }
    end

    private def load_default_definitions
      @log.info { "Loading default definitions" }
      @exchanges[""] = AMQP::DefaultExchange.new(@vhost, "", true, false, false)
      @exchanges["amq.direct"] = AMQP::DirectExchange.new(@vhost, "amq.direct", true, false, false)
      @exchanges["amq.fanout"] = AMQP::FanoutExchange.new(@vhost, "amq.fanout", true, false, false)
      @exchanges["amq.topic"] = AMQP::TopicExchange.new(@vhost, "amq.topic", true, false, false)
      @exchanges["amq.headers"] = AMQP::HeadersExchange.new(@vhost, "amq.headers", true, false, false)
      @exchanges["amq.match"] = AMQP::HeadersExchange.new(@vhost, "amq.match", true, false, false)
    end

    private def compact_locked(all = false)
      return unless all || wal_dirty?

      @log.info { "Compacting definitions" }
      write_exchanges_snapshot if all || @dirty_exchanges
      write_queues_snapshot if all || @dirty_queues
      write_bindings_snapshot if all || @dirty_bindings
      # The snapshot renames must be durable before the WAL is truncated:
      # write_json_snapshot fsyncs each file's content but not the directory
      # entry created by the rename. Without this barrier a crash could leave
      # the old (pre-compaction) snapshots on disk together with an already
      # emptied WAL, permanently losing every change since the last compaction.
      fsync_data_dir
      clear_wal
      @dirty_exchanges = false
      @dirty_queues = false
      @dirty_bindings = false
    end

    private def fsync_data_dir : Nil
      File.open(@data_dir, &.fsync)
    end

    private def write_exchanges_snapshot
      write_json_snapshot(@exchanges_file_path) do |json|
        json.array do
          @exchanges.each_value.select(&.durable?).each do |exchange|
            json.object do
              json.field "name", exchange.name
              json.field "type", exchange.type
              write_true(json, "auto_delete", exchange.auto_delete?)
              write_true(json, "internal", exchange.internal?)
              write_arguments(json, exchange.arguments)
            end
          end
        end
      end
    end

    private def write_queues_snapshot
      write_json_snapshot(@queues_file_path) do |json|
        json.array do
          @queues.each_value.each do |queue|
            next if !queue.durable? || queue.exclusive?
            # Internal delayed-exchange queues are recreated by their exchange
            # on load (register_queue), so the per-frame WAL path never persists
            # them; keep the snapshot consistent and out of the exported defs.
            next if queue.is_a?(AMQP::DelayedExchangeQueue)
            json.object do
              json.field "name", queue.name
              write_true(json, "exclusive", queue.exclusive?)
              write_true(json, "auto_delete", queue.auto_delete?)
              write_arguments(json, queue.arguments)
            end
          end
        end
      end
    end

    private def write_bindings_snapshot
      write_json_snapshot(@bindings_file_path) do |json|
        json.array do
          @exchanges.each_value.each do |exchange|
            next unless exchange.durable?
            exchange.bindings_details.each do |binding|
              destination_type = persisted_destination_type?(binding.destination) || next
              json.object do
                json.field "source", exchange.name
                json.field "destination", binding.destination.name
                json.field "destination_type", destination_type unless destination_type == "queue"
                json.field "routing_key", binding.routing_key unless binding.routing_key.empty?
                write_arguments(json, binding.arguments)
              end
            end
          end
        end
      end
    end

    private def persisted_destination_type?(destination : Destination) : String?
      case destination
      when Queue
        return if !destination.durable? || destination.exclusive?
        "queue"
      when Exchange
        return unless destination.durable?
        "exchange"
      end
    end

    private def write_true(json : JSON::Builder, field : String, value : Bool) : Nil
      json.field(field, true) if value
    end

    private def write_arguments(json : JSON::Builder, arguments : AMQP::Table?) : Nil
      return if arguments.nil?
      return if arguments.empty?

      json.field("arguments") { arguments.to_json(json) }
    end

    private def write_json_snapshot(path : String, &)
      tmpfile = "#{path}.tmp"
      File.open(tmpfile, "w") do |file|
        JSON.build(file) { |json| yield json }
        file.fsync
      end
      File.rename tmpfile, path
      @replicator.try &.replace_file path
    end

    private def clear_wal
      @definitions_file.close
      tmpfile = "#{@definitions_file_path}.tmp"
      File.open(tmpfile, "w", &.fsync)
      File.rename tmpfile, @definitions_file_path
      fsync_data_dir
      @replicator.try &.delete_file @definitions_file_path
      @definitions_file = File.open(@definitions_file_path, "a+")
    end

    private def store_definition(frame, fsync = true)
      @log.debug { "Storing definition: #{frame.inspect}" }
      record = definition_record(frame)
      bytes = record.to_slice
      offset = @definitions_file.size.to_i64
      @definitions_file.write bytes
      @definitions_file.flush
      @definitions_file.fsync if fsync
      @replicator.try &.register_file(@definitions_file) if offset.zero?
      @replicator.try &.append_bytes @definitions_file_path, bytes, offset
      @last_definition_change = RoughTime.instant
      mark_snapshot_dirty(frame)
      if fsync
        # The caller acknowledges the change to the client right after this
        # returns (Declare-Ok etc.), so like a publish confirm it must be
        # durable on every in-sync follower first — otherwise a leader crash
        # could elect a follower lacking the acknowledged change. A follower
        # that doesn't ack within its deadline is disconnected and its ISR
        # removal committed before this returns.
        @replicator.try &.wait_for_followers
      end
      if @definitions_file.size >= WAL_COMPACT_SIZE
        compact_locked
      else
        request_compaction
      end
    end

    private def definition_record(frame) : String
      String.build do |io|
        JSON.build(io) do |json|
          json.object do
            write_wal_fields(frame, json)
          end
        end
        io << '\n'
      end
    end

    private def write_wal_fields(frame, json : JSON::Builder)
      case frame
      when AMQP::Frame::Exchange::Declare
        json.field "op", "exchange.declare"
        json.field "name", frame.exchange_name
        json.field "type", frame.exchange_type
        write_true(json, "auto_delete", frame.auto_delete)
        write_true(json, "internal", frame.internal)
        write_arguments(json, frame.arguments)
      when AMQP::Frame::Exchange::Delete
        json.field "op", "exchange.delete"
        json.field "name", frame.exchange_name
      when AMQP::Frame::Exchange::Bind
        json.field "op", "exchange.bind"
        json.field "destination", frame.destination
        json.field "source", frame.source
        json.field "routing_key", frame.routing_key
        write_arguments(json, frame.arguments)
      when AMQP::Frame::Exchange::Unbind
        json.field "op", "exchange.unbind"
        json.field "destination", frame.destination
        json.field "source", frame.source
        json.field "routing_key", frame.routing_key
        write_arguments(json, frame.arguments)
      when AMQP::Frame::Queue::Declare
        json.field "op", "queue.declare"
        json.field "name", frame.queue_name
        write_true(json, "exclusive", frame.exclusive)
        write_true(json, "auto_delete", frame.auto_delete)
        write_arguments(json, frame.arguments)
      when AMQP::Frame::Queue::Delete
        json.field "op", "queue.delete"
        json.field "name", frame.queue_name
      when AMQP::Frame::Queue::Bind
        json.field "op", "queue.bind"
        json.field "queue", frame.queue_name
        json.field "exchange", frame.exchange_name
        json.field "routing_key", frame.routing_key
        write_arguments(json, frame.arguments)
      when AMQP::Frame::Queue::Unbind
        json.field "op", "queue.unbind"
        json.field "queue", frame.queue_name
        json.field "exchange", frame.exchange_name
        json.field "routing_key", frame.routing_key
        write_arguments(json, frame.arguments)
      else
        raise "Cannot store frame #{frame.class} in vhost #{@vhost.name}"
      end
    end

    private def mark_snapshot_dirty(frame)
      case frame
      when AMQP::Frame::Exchange::Declare
        @dirty_exchanges = true
      when AMQP::Frame::Exchange::Delete
        @dirty_exchanges = true
        @dirty_bindings = true
      when AMQP::Frame::Exchange::Bind, AMQP::Frame::Exchange::Unbind
        @dirty_bindings = true
      when AMQP::Frame::Queue::Declare
        @dirty_queues = true
      when AMQP::Frame::Queue::Delete
        @dirty_queues = true
        @dirty_bindings = true
      when AMQP::Frame::Queue::Bind, AMQP::Frame::Queue::Unbind
        @dirty_bindings = true
      end
    end

    private def request_compaction
      @compact_requested.try_send nil
    rescue Channel::ClosedError
    end

    private def compact_loop
      compact_at = nil
      loop do
        select
        when @compact_requested.receive
          compact_at = next_compact_at
        when timeout compact_timeout(compact_at)
          if compact_at
            @definitions_lock.synchronize { compact_locked }
            compact_at = nil
          end
        end
      end
    rescue Channel::ClosedError
    end

    private def next_compact_at : Time::Instant
      last_definition_change, definitions_file_size = @definitions_lock.synchronize do
        {@last_definition_change, @definitions_file.size}
      end
      last_definition_change + compact_delay(definitions_file_size)
    end

    private def compact_delay(size : Int) : Time::Span
      return Time::Span.zero if size >= WAL_COMPACT_SIZE

      # Wait the full idle window when the WAL is small so bursts of declares
      # batch into one compaction, and shrink the delay toward zero as the WAL
      # approaches the size cap so a fast-growing WAL is compacted promptly.
      WAL_COMPACT_IDLE * (1.0 - size.to_f64 / WAL_COMPACT_SIZE)
    end

    private def compact_timeout(compact_at : Time::Instant?) : Time::Span
      return 1.day unless compact_at

      remaining = compact_at - RoughTime.instant
      remaining > Time::Span.zero ? remaining : Time::Span.zero
    end

    private def make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        if name.empty?
          AMQP::DefaultExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        else
          AMQP::DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
        end
      when "fanout"
        AMQP::FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        AMQP::TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        AMQP::HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "x-delayed-message", "x-delayed-exchange"
        arguments = arguments.clone
        type = arguments.delete("x-delayed-type")
        raise Error::ExchangeTypeError.new("Missing required argument 'x-delayed-type'") unless type
        arguments["x-delayed-exchange"] = true
        make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      when "x-federation-upstream"
        AMQP::FederationExchange.new(vhost, name, arguments)
      when "x-consistent-hash"
        AMQP::ConsistentHashExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      else raise Error::ExchangeTypeError.new("unknown exchange type '#{type}'")
      end
    end
  end
end
