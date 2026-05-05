require "./logger"
require "./schema"
require "./event_type"
require "./queue_factory"
require "./amqp/exchange/*"
require "./amqp/queue"

module LavinMQ
  class DefinitionsStore
    Log = LavinMQ::Log.for "definitions_store"

    def initialize(@vhost : VHost, @data_dir : String, @replicator : Clustering::Replicator?, @log : Logger)
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @definitions_lock = Mutex.new(:reentrant)
      @definitions_file_path = File.join(@data_dir, "definitions.amqp")
      @definitions_file = File.open(@definitions_file_path, "a+")
      @replicator.try &.register_file(@definitions_file)
      @definitions_deletes = 0
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

    def exchanges_dup : Array(Exchange)
      @exchanges.values
    end

    def exchanges_size : Int32
      @exchanges.size
    end

    def exchanges_any?(& : {String, Exchange} -> Bool) : Bool
      @exchanges.any? { |kv| yield kv }
    end

    def exchanges_unsafe_put(name : String, exchange : Exchange) : Nil
      @exchanges[name] = exchange
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

    def queues_dup : Array(Queue)
      @queues.values
    end

    def queues_size : Int32
      @queues.size
    end

    def queues_values : Array(Queue)
      @queues.values
    end

    def queues_unsafe_put(name : String, queue : Queue) : Nil
      @queues[name] = queue
    end

    def queues_clear : Nil
      @queues.clear
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def apply(f, loading = false) : Bool
      @definitions_lock.synchronize do
        case f
        when AMQP::Frame::Exchange::Declare
          return false if @exchanges.has_key? f.exchange_name
          e = @exchanges[f.exchange_name] =
            make_exchange(@vhost, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
          @vhost.apply_policies([e] of Exchange) unless loading
          store_definition(f) if !loading && f.durable
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
            store_definition(f, dirty: true) if !loading && x.durable?
          else
            return false
          end
        when AMQP::Frame::Exchange::Bind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.bind(dst, f.routing_key, f.arguments)
          store_definition(f) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Exchange::Unbind
          src = @exchanges[f.source]? || return false
          dst = @exchanges[f.destination]? || return false
          return false unless src.unbind(dst, f.routing_key, f.arguments)
          store_definition(f, dirty: true) if !loading && src.durable? && dst.durable?
        when AMQP::Frame::Queue::Declare
          return false if @queues.has_key? f.queue_name
          q = @queues[f.queue_name] = QueueFactory.make(@vhost, f)
          @vhost.apply_policies([q] of Queue) unless loading
          store_definition(f) if !loading && f.durable && !f.exclusive
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
            store_definition(f, dirty: true) if !loading && q.durable? && !q.exclusive?
            @vhost.event_tick(EventType::QueueDeleted) unless loading
            q.delete
          else
            return false
          end
        when AMQP::Frame::Queue::Bind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.bind(q, f.routing_key, f.arguments)
          store_definition(f) if !loading && x.durable? && q.durable? && !q.exclusive?
        when AMQP::Frame::Queue::Unbind
          x = @exchanges[f.exchange_name]? || return false
          q = @queues[f.queue_name]? || return false
          return false unless x.unbind(q, f.routing_key, f.arguments)
          store_definition(f, dirty: true) if !loading && x.durable? && q.durable? && !q.exclusive?
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

    # ameba:disable Metrics/CyclomaticComplexity
    def load!
      exchanges = Hash(String, AMQP::Frame::Exchange::Declare).new
      queues = Hash(String, AMQP::Frame::Queue::Declare).new
      queue_bindings = Hash(String, Array(AMQP::Frame::Queue::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Queue::Bind).new }
      exchange_bindings = Hash(String, Array(AMQP::Frame::Exchange::Bind)).new { |h, k| h[k] = Array(AMQP::Frame::Exchange::Bind).new }
      should_compact = false
      io = @definitions_file
      if io.size.zero?
        load_default_definitions
        compact!
        return
      end

      @log.info { "Loading definitions" }
      @definitions_lock.synchronize do
        @log.debug { "Verifying schema" }
        SchemaVersion.verify(io, :definition)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        loop do
          f = stream.next_frame
          @log.trace { "Reading frame #{f.inspect}" }
          case f
          when AMQP::Frame::Exchange::Declare
            exchanges[f.exchange_name] = f
          when AMQP::Frame::Exchange::Delete
            exchanges.delete f.exchange_name
            exchange_bindings.delete f.exchange_name
            should_compact = true
          when AMQP::Frame::Exchange::Bind
            exchange_bindings[f.destination] << f
          when AMQP::Frame::Exchange::Unbind
            exchange_bindings[f.destination].reject! do |b|
              b.source == f.source &&
                b.routing_key == f.routing_key &&
                b.arguments == f.arguments
            end
            should_compact = true
          when AMQP::Frame::Queue::Declare
            queues[f.queue_name] = f
          when AMQP::Frame::Queue::Delete
            queues.delete f.queue_name
            queue_bindings.delete f.queue_name
            should_compact = true
          when AMQP::Frame::Queue::Bind
            queue_bindings[f.queue_name] << f
          when AMQP::Frame::Queue::Unbind
            queue_bindings[f.queue_name].reject! do |b|
              b.exchange_name == f.exchange_name &&
                b.routing_key == f.routing_key &&
                b.arguments == f.arguments
            end
            should_compact = true
          else
            raise "Cannot apply frame #{f.class} in vhost #{@vhost.name}"
          end
        rescue ex : IO::EOFError
          break
        end
      end

      @log.info { "Applying #{exchanges.size} exchanges" }
      exchanges.each_value &->self.load_apply(AMQP::Frame)
      @log.info { "Applying #{queues.size} queues" }
      queues.each_value &->self.load_apply(AMQP::Frame)
      @log.info { "Applying #{exchange_bindings.each_value.sum(0, &.size)} exchange bindings" }
      exchange_bindings.each_value &.each(&->self.load_apply(AMQP::Frame))
      @log.info { "Applying #{queue_bindings.each_value.sum(0, &.size)} queue bindings" }
      queue_bindings.each_value &.each(&->self.load_apply(AMQP::Frame))

      @log.info { "Definitions loaded" }
      compact! if should_compact
    end

    def close : Nil
      @definitions_file.close
    end

    def sync : Nil
      {% if flag?(:linux) %}
        ret = LibC.syncfs(@definitions_file.fd)
        raise IO::Error.from_errno("syncfs") if ret != 0
      {% else %}
        LibC.sync
      {% end %}
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

    private def compact!
      @definitions_lock.synchronize do
        @log.info { "Compacting definitions" }
        io = File.open("#{@definitions_file_path}.tmp", "a+")
        SchemaVersion.prefix(io, :definition)
        @exchanges.each_value.select(&.durable?).each do |e|
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable?, e.auto_delete?, e.internal?,
            false, e.arguments)
          io.write_bytes f
        end
        @queues.each_value.select(&.durable?).each do |q|
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable?, q.exclusive?,
            q.auto_delete?, false, q.arguments)
          io.write_bytes f
        end
        @exchanges.each_value.select(&.durable?).each do |e|
          e.bindings_details.each do |binding|
            args = binding.arguments || AMQP::Table.new
            frame = case binding.destination
                    when Queue
                      AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, binding.destination.name, e.name,
                        binding.routing_key, false, args)
                    when Exchange
                      AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, binding.destination.name, e.name,
                        binding.routing_key, false, args)
                    end
            if f = frame
              io.write_bytes f
            end
          end
        end
        io.fsync
        File.rename io.path, @definitions_file_path
        @replicator.try &.replace_file @definitions_file_path
        @definitions_file.close
        @definitions_file = io
      end
    end

    private def store_definition(frame, dirty = false)
      @log.debug { "Storing definition: #{frame.inspect}" }
      bytes = frame.to_slice
      @definitions_file.write bytes
      @replicator.try &.append @definitions_file_path, bytes
      @definitions_file.fsync
      if dirty
        if (@definitions_deletes += 1) >= Config.instance.max_deleted_definitions
          compact!
          @definitions_deletes = 0
        end
      end
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
