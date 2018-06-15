require "json"
require "logger"
require "./amqp/io"
require "./segment_position"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel_store"
require "digest/sha1"

module AvalancheMQ
  class VHost
    class MessageFile < File
      include AMQP::IO
    end

    getter name, exchanges, queues, log, data_dir, policies, parameters, log, shovels

    MAX_SEGMENT_SIZE = 256 * 1024**2
    @segment : UInt32
    @wfile : MessageFile
    @log : Logger
    @shovels : ShovelStore?

    def initialize(@name : String, @server_data_dir : String, server_log : Logger)
      @log = server_log.dup
      @log.progname = "vhost=#{@name}"
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Frame).new(32)
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p @data_dir
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      @segment = last_segment
      @wfile = open_wfile
      @shovels = ShovelStore.new(self)
      load!
      compact!
      spawn save!, name: "VHost#save!"
    end

    def publish(msg : Message, immediate = false) : Bool
      try_publish(msg, immediate) || send_to_alternate_exchange?(msg, immediate)
    end

    private def try_publish(msg : Message, immediate = false) : Bool
      ex = @exchanges[msg.exchange_name]?
      return false if ex.nil?
      ok = false

      matches = ex.matches(msg.routing_key, msg.properties.headers)
      if cc = msg.properties.headers.try(&.fetch("CC", nil))
        cc.as(Array(AMQP::Field)).each do |rk|
          matches.concat(ex.matches(rk.as(String), msg.properties.headers))
        end
      end
      if bcc = msg.properties.headers.try(&.delete("BCC"))
        bcc.as(Array(AMQP::Field)).each do |rk|
          matches.concat(ex.matches(rk.as(String), msg.properties.headers))
        end
      end

      return false if matches.empty?
      exchanges = matches.compact_map { |m| m.as? Exchange }
      queues = matches.compact_map { |m| m.as? Queue }
      ok = exchanges.map do |e|
        emsg = msg.dup
        emsg.exchange_name = e.name
        publish(emsg, immediate).as Bool
      end.any?
      return ok if immediate && (queues.empty? || queues.any? { |q| !q.immediate_delivery? })

      pos = @wfile.pos.to_u32

      if pos >= MAX_SEGMENT_SIZE
        @segment += 1
        @wfile.close
        @wfile = open_wfile
        pos = 0_u32
        spawn gc_segments!, name: "GC Segments #{@name}"
      end

      sp = SegmentPosition.new(@segment, pos)
      @wfile.write_int msg.timestamp
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_bytes msg.properties
      @wfile.write_int msg.size
      @wfile.write msg.body
      @wfile.flush
      flush = msg.properties.delivery_mode == 2_u8
      return queues.map { |q| q.publish(sp, flush) }.any? || ok
    end

    private def send_to_alternate_exchange?(msg, immediate = false, visited = [] of String)
      if ae = @exchanges[msg.exchange_name]?.try &.alternate_exchange
        visited.push(msg.exchange_name)
        unless visited.includes?(ae)
          ae_msg = msg.dup
          ae_msg.exchange_name = ae
          ok = publish(ae_msg, immediate)
          return send_to_alternate_exchange?(ae_msg, immediate, visited) unless ok
        end
      end
      return false
    end

    private def open_wfile : MessageFile
      @log.debug { "Opening message store segment #{@segment}" }
      filename = "msgs.#{@segment.to_s.rjust(10, '0')}"
      wfile = MessageFile.open(File.join(@data_dir, filename), "a")
      wfile.seek(0, IO::Seek::End)
      wfile
    end

    def to_json(json : JSON::Builder)
      vhost_details.to_json(json)
    end

    def vhost_details
      {
        name: @name,
        dir:  @dir,
      }
    end

    def message_details
      ready = @queues.values.reduce(0) { |m, q| m += q.message_count }
      unacked = @queues.values.reduce(0) { |m, q| m += q.unacked_count }
      {
        messages:                ready + unacked,
        messages_unacknowledged: unacked,
        messages_ready:          ready,
      }
    end

    def declare_queue(name, durable, auto_delete,
                      arguments = Hash(String, AMQP::Field).new)
      apply AMQP::Queue::Declare.new(0_u16, 0_u16, name, false, durable, false,
        auto_delete, false, arguments)
    end

    def delete_queue(name)
      apply AMQP::Queue::Delete.new(0_u16, 0_u16, name, false, false, false)
    end

    def declare_exchange(name, type, durable, auto_delete, internal = false,
                         arguments = Hash(String, AMQP::Field).new)
      apply AMQP::Exchange::Declare.new(0_u16, 0_u16, name, type, false, durable,
        auto_delete, internal, false, arguments)
    end

    def delete_exchange(name)
      apply AMQP::Exchange::Delete.new(0_u16, 0_u16, name, false, false)
    end

    def bind_queue(destination, source, routing_key, arguments = Hash(String, AMQP::Field).new)
      apply AMQP::Queue::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
    end

    def bind_exchange(destination, source, routing_key, arguments = Hash(String, AMQP::Field).new)
      apply AMQP::Exchange::Bind.new(0_u16, 0_u16, destination, source,
        routing_key, false, arguments)
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
            destination.delete f.exchange_name
          end
        end
        @exchanges.delete f.exchange_name
      when AMQP::Exchange::Bind
        source = @exchanges[f.source]? || return
        x = @exchanges[f.destination]? || return
        source.bind(x, f.routing_key, f.arguments)
      when AMQP::Exchange::Unbind
        source = @exchanges[f.source]? || return
        x = @exchanges[f.destination]? || return
        source.unbind(x, f.routing_key, f.arguments)
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
        x = @exchanges[f.exchange_name]? || return
        q = @queues[f.queue_name]? || return
        x.bind(q, f.routing_key, f.arguments)
      when AMQP::Queue::Unbind
        x = @exchanges[f.exchange_name]? || return
        q = @queues[f.queue_name]? || return
        x.unbind(q, f.routing_key, f.arguments)
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
    end

    def add_policy(name : String, pattern : Regex, apply_to : Policy::Target,
                   definition : JSON::Any, priority : Int8)
      add_policy(Policy.new(name, @name, pattern, apply_to, definition, priority))
    end

    def add_policy(p : Policy)
      @policies.create(p)
      spawn apply_policies, name: "ApplyPolicies (after add) #{@name}"
    end

    def delete_policy(name)
      @policies.delete(name)
      spawn apply_policies, name: "ApplyPolicies (after delete) #{@name}"
    end

    def add_parameter(p : Parameter)
      @parameters.create p
      apply_parameters(p)
    end

    def delete_parameter(component_name, parameter_name)
      @parameters.delete({component_name, parameter_name})
      case component_name
      when "shovel"
        @shovels.not_nil!.delete(parameter_name)
      else
        @log.warn("No action when deleting parameter #{component_name}")
      end
    end

    def stop_shovels
      @shovels.not_nil!.each &.stop
    end

    def close
      stop_shovels
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

    private def apply_parameters(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        case p.component_name
        when "shovel"
          @shovels.not_nil!.create(p.parameter_name, p.value)
        else
          @log.warn("No action when applying parameter #{p.component_name}")
        end
      end
    end

    private def load!
      load_definitions!
      apply_policies
      apply_parameters
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
      return unless Dir.exists?(@data_dir)
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
end
