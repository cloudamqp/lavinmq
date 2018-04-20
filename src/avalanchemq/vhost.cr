require "json"
require "logger"
require "./amqp/io"
require "./segment_position"
require "./policy"
require "./parameter_store"
require "./parameter"
require "digest/sha1"

module AvalancheMQ
  class VHost
    class MessageFile < File
      include AMQP::IO
    end
    getter name, exchanges, queues, log, data_dir, policies

    MAX_SEGMENT_SIZE = 256 * 1024**2
    @segment : UInt32
    @wfile : MessageFile
    @log : Logger

    def initialize(@name : String, @server_data_dir : String, server_log : Logger)
      @log = server_log.dup
      @log.progname = "VHost[#{@name}]"
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Frame).new(32)
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p @data_dir
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @segment = last_segment
      @wfile = open_wfile
      load!
      compact!
      spawn save!, name: "VHost#save!"
    end

    def publish(msg : Message, immediate = false)
      ex = @exchanges[msg.exchange_name]?
      raise MessageUnroutableError.new if ex.nil?

      ok = false
      matches = ex.matches(msg.routing_key, msg.properties.headers)
      exchanges = matches.compact_map { |m| m.as? Exchange }
      queues = matches.compact_map { |m| m.as? Queue }
      ok = exchanges.map do |e|
        emsg = msg.dup
        emsg.exchange_name = e.name
        publish(emsg, immediate).as(Bool)
      end.all?
      raise MessageUnroutableError.new if matches.empty?

      pos = @wfile.pos.to_u32

      if pos >= MAX_SEGMENT_SIZE
        @segment += 1
        @wfile.close
        @wfile = open_wfile
        pos = 0_u32
        spawn gc_segments!
      end

      sp = SegmentPosition.new(@segment, pos)
      @wfile.write_int msg.timestamp
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_bytes msg.properties
      @wfile.write_int msg.size
      @wfile.write msg.body
      @wfile.flush
      if immediate
        raise NoImmediateDeliveryError.new if queues.any? { |q| !q.immediate_delivery? }
      end
      flush = msg.properties.delivery_mode == 2_u8
      queues.all? { |q| q.publish(sp, flush) } && ok
    end

    private def open_wfile : MessageFile
      @log.debug { "Opening message store segment #{@segment}" }
      filename = "msgs.#{@segment.to_s.rjust(10, '0')}"
      wfile = MessageFile.open(File.join(@data_dir, filename), "a")
      wfile.seek(0, IO::Seek::End)
      wfile
    end

    def to_json(json : JSON::Builder)
      {
        name: @name,
        dir: @dir
      }.to_json(json)
    end

    def apply(f, loading = false)
      @save.send f unless loading
      case f
      when AMQP::Exchange::Declare
        e = @exchanges[f.exchange_name] =
          Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
        apply_policies([e] of Exchange)
      when AMQP::Exchange::Delete
        @exchanges.each_value do |e|
          e.bindings.each_value do |destination|
            destination.delete @exchanges[f.exchange_name]
          end
        end
        @exchanges.delete f.exchange_name
      when AMQP::Exchange::Bind
        x = @exchanges[f.destination]? || return
        @exchanges[f.source].bind(x, f.routing_key, f.arguments)
      when AMQP::Exchange::Unbind
        x = @exchanges[f.destination]? || return
        @exchanges[f.source].unbind(x, f.routing_key, f.arguments)
      when AMQP::Queue::Declare
        q = @queues[f.queue_name] =
          if f.durable
            DurableQueue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments)
          else
            Queue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments)
          end
        @exchanges[""].bind(q, f.queue_name, f.arguments)
        apply_policies([q] of Queue)
      when AMQP::Queue::Delete
        q = @queues.delete(f.queue_name)
        @exchanges.each_value do |e|
          e.bindings.each_value do |destinations|
            destinations.delete q
          end
        end
        q.try &.close
      when AMQP::Queue::Bind
        q = @queues[f.queue_name]? || return
        @exchanges[f.exchange_name].bind(q, f.routing_key, f.arguments)
      when AMQP::Queue::Unbind
        q = @queues[f.queue_name]? || return
        @exchanges[f.exchange_name].unbind(q, f.routing_key, f.arguments)
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
    end

    def add_policy(name : String, pattern : Regex, apply_to : Policy::Target,
                   definition : JSON::Any, priority : Int8)
      add_policy(Policy.new(name, @name, pattern, apply_to, definition, priority))
    end

    def add_policy(p : Policy)
      @policies.create(p)
      spawn apply_policies
    end

    def delete_policy(name)
      @policies.delete(name)
      spawn apply_policies
    end

    def close
      @queues.each_value &.close
      @save.close
    end

    def delete
      close
      Fiber.yield
      Dir.children(@data_dir).each { |f| File.delete File.join(@data_dir, f) }
      Dir.rmdir @data_dir
    end

    private def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      itr = if resources
              resources.each
            else
              @queues.values.each.chain(@exchanges.values.each)
            end
      sorted_policies = @policies.values.sort_by!(&.priority).reverse
      itr.each do |r|
        match = sorted_policies.find { |p| p.match?(r) }
        r.apply_policy(match) unless match.nil?
      end
    end

    private def load!
      load_definitions!
    end

    private def load_definitions!
      File.open(File.join(@data_dir, "definitions.amqp"), "r") do |io|
        loop do
          begin
            apply AMQP::Frame.decode(io), loading: true
          rescue ex : AMQP::FrameDecodeError
            break if ex.cause.is_a? IO::EOFError
            raise ex
          end
        end
      end
    rescue Errno
      load_default_definitions
    end

    private def load_default_definitions
      @log.info "Loading default definitions"
      @exchanges[""] = DirectExchange.new(self, "", true, false, true)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", true, false, true)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", true, false, true)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", true, false, true)
      @exchanges["amq.headers"] = HeadersExchange.new(self, "amq.headers", true, false, true)
      @exchanges["amq.match"] = HeadersExchange.new(self, "amq.match", true, false, true)
    end

    private def compact!
      @log.debug "Compacting definitions"
      tmp_path = File.join(@data_dir, "definitions.amqp.tmp")
      File.delete tmp_path if File.exists? tmp_path
      File.open(tmp_path, "w") do |io|
        @exchanges.each do |name, e|
          next unless e.durable
          next if e.auto_delete
          f = AMQP::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
                                          false, e.durable, e.auto_delete, e.internal,
                                          false, e.arguments)
          f.encode(io)
          e.bindings.each do |bt, destinations|
            destinations.each do |d|
              f =
                case d
                when Queue
                  AMQP::Queue::Bind.new(0_u16, 0_u16, d.name, e.name, bt[0], false, bt[1])
                when Exchange
                  AMQP::Exchange::Bind.new(0_u16, 0_u16, e.name, d.name, bt[0], false, bt[1])
                else raise "Unknown destination type #{d.class}"
                end
              f.encode(io)
            end
          end
        end
        @queues.each do |name, q|
          next unless q.durable
          next if q.auto_delete # FIXME: Auto delete should be persistet, but also deleted
          f = AMQP::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
                                       q.auto_delete, false, q.arguments)
          f.encode(io)
        end
      end
      File.rename tmp_path, File.join(@data_dir, "definitions.amqp")
    end

    private def save!
      File.open(File.join(@data_dir, "definitions.amqp"), "a") do |f|
        loop do
          frame = @save.receive
          case frame
          when AMQP::Exchange::Declare
            next unless frame.durable
          when AMQP::Queue::Declare
            next unless frame.durable
            next if frame.exclusive
          when AMQP::Exchange::Delete
            next unless @exchanges[frame.exchange_name]?.try(&.durable)
          when AMQP::Queue::Delete
            next unless @queues[frame.queue_name]?.try { |q| q.durable && !q.exclusive }
          when AMQP::Queue::Bind, AMQP::Queue::Unbind
            next unless @exchanges[frame.exchange_name]?.try(&.durable)
            q = @queues[frame.queue_name]
            next unless q.durable && !q.exclusive
          when AMQP::Exchange::Bind, AMQP::Exchange::Unbind
            s = @exchanges[frame.source]
            next unless s.durable
            d = @exchanges[frame.destination]
            next unless d.durable
          else raise "Cannot apply frame #{frame.class} in vhost #{@name}"
          end
          @log.debug { "Storing definition: #{f.inspect}" }
          frame.encode(f)
          f.flush
        end
      end
      @policies.save!
    rescue Channel::ClosedError
      @log.debug "Save channel closed"
    ensure
      @save.close
    end

    private def last_segment : UInt32
      segments = Dir.glob(File.join(@data_dir, "msgs.*")).sort
      last_file = segments.last? || return 0_u32
      segment = File.basename(last_file)[5, 10].to_u32
      @log.debug { "Last segment is #{segment}" }
      segment
    end

    private def gc_segments!
      @log.info "Garbage collecting segments"
      referenced_segments = Set(UInt32).new([@segment])
      @queues.each_value do |q|
        used = q.close_unused_segments_and_report_used
        referenced_segments.concat used
      end
      @log.info "#{referenced_segments.size} segments in use"

      Dir.glob(File.join(@data_dir, "msgs.*")).each do |f|
        seg = File.basename(f)[5, 10].to_u32
        next if referenced_segments.includes? seg
        @log.info "Deleting segment #{seg}"
        File.delete f
      end
    end
  end

  class MessageUnroutableError < Exception; end
  class NoImmediateDeliveryError < Exception; end
end
