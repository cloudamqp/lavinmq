require "json"
require "logger"
require "./segment_position"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/shovel_store"
require "./federation/upstream_store"
require "./client/direct_client"
require "./sortable_json"
require "./durable_queue"
require "./exchange"
require "digest/sha1"

module AvalancheMQ
  class VHost
    include SortableJSON
    getter name, exchanges, queues, log, data_dir, policies, parameters, log, shovels,
      direct_reply_channels, upstreams, default_user
    property? flow = true

    MAX_SEGMENT_SIZE = Config.instance.segment_size
    @segment : UInt32
    @wfile : File
    @log : Logger
    @direct_reply_channels = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    EXCHANGE_TYPES = %w(direct fanout topic headers x-federation-upstream)

    def_equals_and_hash @name

    def initialize(@name : String, @server_data_dir : String,
                   server_log : Logger, @default_user : User,
                   @connection_events = Server::ConnectionsEvents.new(16))
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
      @wfile.seek(0, IO::Seek::End)
      @pos = @wfile.pos.to_u32
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      @fsync = false
      load!
      compact!
      spawn save!, name: "VHost/#{@name}#save!"
      spawn publish_loop, name: "VHost/#{@name}#publish_loop"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << ">"
    end

    @awaiting_confirm = Set(Client::Channel).new

    def waiting4confirm(channel)
      @fsync = true
      @awaiting_confirm.add channel
    end

    @incoming = Channel(Tuple(Message, Bool, Bool)).new
    @outgoing = Channel(Bool).new

    def publish(msg : Message, immediate = false, confirm = false)
      @incoming.send({ msg, immediate, confirm })
      @outgoing.receive
    end

    @queues_to_fsync = Set(DurableQueue).new

    def fsync
      return unless @fsync
      @log.debug { "fsync" }
      @wfile.fsync(flush_metadata: false)
      @queues_to_fsync.each &.fsync_enq
      @awaiting_confirm.each do |ch|
        ch.confirm_ack(multiple: true)
      end
      @awaiting_confirm.clear
      @fsync = false
      spawn do
        sleep 0.2
        fsync
      end
    end

    def publish_loop
      loop do
        fsync if @incoming.empty?
        msg, immediate, confirm = @incoming.receive
        ok = actual_publish(msg, immediate, confirm)
        @outgoing.send(ok)
      rescue Channel::ClosedError
        break
      end
    end

    @visited = Set(Exchange).new
    @found_queues = Set(Queue).new

    def actual_publish(msg, immediate, confirm) : Bool
      ex = @exchanges[msg.exchange_name]? || return false
      ex.publish_in_count += 1
      queues = find_all_queues(ex, msg.routing_key, msg.properties.headers, @visited, @found_queues)
      @log.debug { "publish queues#found=#{queues.size}" }
      return false if queues.empty?
      return false if immediate && !queues.any? { |q| q.immediate_delivery? }
      sp = write_to_disk(msg)
      flush = msg.properties.delivery_mode == 2_u8
      ok = false
      queues.each do |q|
        ex.publish_out_count += 1
        if q.publish(sp, flush)
          @queues_to_fsync << q if q.is_a?(DurableQueue) && flush
          ok = true
        end
      end
      @visited.clear
      @found_queues.clear
      ok
    end

    private def find_all_queues(ex : Exchange, routing_key : String,
                                headers : AMQP::Table?,
                                visited = Set(Exchange).new,
                                queues = Set(Queue).new) : Set(Queue)
      matches = ex.matches(routing_key, headers)
      if cc = headers.try(&.fetch("CC", nil))
        cc.as(Array(AMQP::Field)).each do |rk|
          matches.concat(ex.matches(rk.as(String), headers))
        end
      end
      if bcc = headers.try(&.delete("BCC"))
        bcc.as(Array(AMQP::Field)).each do |rk|
          matches.concat(ex.matches(rk.as(String), headers))
        end
      end

      matches.each do |m|
        case m
        when Queue
          queues.add m.as(Queue)
        when Exchange
          e2e = m.as(Exchange)
          visited.add(ex)
          unless visited.includes? e2e
            find_all_queues(e2e, routing_key, headers, visited, queues)
          end
        end
      end

      if queues.empty? && ex.alternate_exchange
        if ae = @exchanges[ex.alternate_exchange]?
            visited.add(ex)
          unless visited.includes?(ae)
            find_all_queues(ae, routing_key, headers, visited, queues)
          end
        end
      end
      queues
    end

    @pos = 0_u32

    private def write_to_disk(msg) : SegmentPosition
      if @pos >= MAX_SEGMENT_SIZE
        @segment += 1
        fsync
        @wfile.close
        @wfile = open_wfile
        spawn gc_segments!, name: "GC Segments #{@name}"
      end

      sp = SegmentPosition.new(@segment, @pos)
      @log.debug { "Writing message: exchange=#{msg.exchange_name} routing_key=#{msg.routing_key} \
                    size=#{msg.bytesize} sp=#{sp}" }
      @wfile.write_bytes msg.timestamp, IO::ByteFormat::NetworkEndian
      @wfile.write_bytes AMQP::ShortString.new(msg.exchange_name), IO::ByteFormat::NetworkEndian
      @wfile.write_bytes AMQP::ShortString.new(msg.routing_key), IO::ByteFormat::NetworkEndian
      @wfile.write_bytes msg.properties, IO::ByteFormat::NetworkEndian
      @wfile.write_bytes msg.size, IO::ByteFormat::NetworkEndian
      copied = IO.copy(msg.body_io, @wfile, msg.size)
      if copied != msg.size
        raise IO::Error.new("Could only write #{copied} of #{msg.size} bytes to message store")
      end
      @wfile.flush
      @pos += msg.bytesize
      sp
    rescue ex
      @log.error "Rotating segment because failed to write message"
      @segment += 1
      @wfile.close
      @wfile = open_wfile
      raise ex
    end

    private def open_wfile : File
      @log.debug { "Opening message store segment #{@segment}" }
      filename = "msgs.#{@segment.to_s.rjust(10, '0')}"
      File.open(File.join(@data_dir, filename), "a").tap do |f|
        f.buffer_size = Config.instance.file_buffer_size
        f.hint_target_size(MAX_SEGMENT_SIZE)
        @pos = 0_u32
      end
    end

    def details_tuple
      {
        name: @name,
        dir:  @dir,
      }
    end

    def message_details
      ready = @queues.each_value.reduce(0) { |m, q| m += q.message_count; m }
      unacked = @queues.each_value.reduce(0) { |m, q| m += q.unacked_count; m }
      {
        messages:                ready + unacked,
        messages_unacknowledged: unacked,
        messages_ready:          ready,
      }
    end

    def declare_queue(name, durable, auto_delete,
                      arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, durable, false,
                                            auto_delete, false, arguments)
    end

    def delete_queue(name)
      apply AMQP::Frame::Queue::Delete.new(0_u16, 0_u16, name, false, false, false)
      GC.collect
    end

    def declare_exchange(name, type, durable, auto_delete, internal = false,
                         arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, name, type, false, durable,
                                               auto_delete, internal, false, arguments)
    end

    def delete_exchange(name)
      apply AMQP::Frame::Exchange::Delete.new(0_u16, 0_u16, name, false, false)
    end

    def bind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, destination, source,
                                         routing_key, false, arguments)
    end

    def bind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, destination, source,
                                            routing_key, false, arguments)
    end

    def unbind_queue(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Queue::Unbind.new(0_u16, 0_u16, destination, source,
                                           routing_key, arguments)
    end

    def unbind_exchange(destination, source, routing_key, arguments = AMQP::Table.new)
      apply AMQP::Frame::Exchange::Unbind.new(0_u16, 0_u16, destination, source,
                                              routing_key, false, arguments)
    end

    def apply(f, loading = false) : Bool?
      case f
      when AMQP::Frame::Exchange::Declare
        return if @exchanges.has_key? f.exchange_name
        e = @exchanges[f.exchange_name] =
          Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments.to_h)
        apply_policies([e] of Exchange) unless loading
      when AMQP::Frame::Exchange::Delete
        return unless @exchanges.has_key? f.exchange_name
        @exchanges.each_value do |ex|
          ex.bindings.each_value do |destination|
            destination.delete f.exchange_name
          end
        end
        @exchanges.delete f.exchange_name
      when AMQP::Frame::Exchange::Bind
        source = @exchanges[f.source]? || return
        x = @exchanges[f.destination]? || return
        source.bind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Exchange::Unbind
        source = @exchanges[f.source]? || return
        x = @exchanges[f.destination]? || return
        source.unbind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Declare
        return if @queues.has_key? f.queue_name
        q = @queues[f.queue_name] =
          if f.durable
            DurableQueue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments.to_h)
          else
            Queue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments.to_h)
          end
        @exchanges[""].bind(q, f.queue_name, f.arguments.to_h)
        apply_policies([q] of Queue) unless loading
      when AMQP::Frame::Queue::Delete
        return unless @queues.has_key? f.queue_name
        q = @queues.delete(f.queue_name)
        # TODO optimize
        @exchanges.each_value do |ex|
          ex.bindings.each_value do |destinations|
            destinations.delete q
          end
        end
        q.try &.delete
      when AMQP::Frame::Queue::Bind
        x = @exchanges[f.exchange_name]? || return
        q = @queues[f.queue_name]? || return
        x.bind(q, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Unbind
        x = @exchanges[f.exchange_name]? || return
        q = @queues[f.queue_name]? || return
        x.unbind(q, f.routing_key, f.arguments.to_h)
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
      @save.send f unless loading
      true
    end

    def add_policy(name : String, pattern : Regex, apply_to : Policy::Target,
                   definition : Hash(String, JSON::Any), priority : Int8)
      add_policy(Policy.new(name, @name, pattern, apply_to, definition, priority))
    end

    def add_policy(p : Policy)
      @policies.delete(p.name)
      @policies.create(p)
      spawn apply_policies, name: "ApplyPolicies (after add) #{@name}"
    end

    def delete_policy(name)
      @policies.delete(name)
      spawn apply_policies, name: "ApplyPolicies (after delete) #{@name}"
    end

    def add_connection(client : Client)
      @connection_events.send({client, :connected})
      client.on_close do |c|
        @connection_events.send({c, :disconnected})
      end
    end

    SHOVEL                  = "shovel"
    FEDERATION_UPSTREAM     = "federation-upstream"
    FEDERATION_UPSTREAM_SET = "federation-upstream-set"

    def add_parameter(p : Parameter)
      @parameters.delete(p.name)
      @parameters.create(p)
      apply_parameters(p)
      spawn apply_policies, name: "ApplyPolicies (add parameter) #{@name}"
    end

    def delete_parameter(component_name, parameter_name)
      @parameters.delete({component_name, parameter_name})
      case component_name
      when SHOVEL
        @shovels.not_nil!.delete(parameter_name)
      when FEDERATION_UPSTREAM
        @upstreams.not_nil!.delete_upstream(parameter_name)
      when FEDERATION_UPSTREAM_SET
        @upstreams.not_nil!.delete_upstream_set(parameter_name)
      else
        @log.warn { "No action when deleting parameter #{component_name}" }
      end
    end

    def stop_shovels
      @shovels.not_nil!.each_value &.stop
    end

    def stop_upstream_links
      @upstreams.not_nil!.stop_all
    end

    def close
      @log.info("Closing")
      stop_shovels
      Fiber.yield
      stop_upstream_links
      Fiber.yield
      @queues.each_value &.close
      Fiber.yield
      @incoming.close
      Fiber.yield
      @outgoing.close
      @save.close
      Fiber.yield
      compact!
      @wfile.close
    end

    def delete
      close
      Fiber.yield
      FileUtils.rm_rf @data_dir
    end

    private def apply_policies(resources : Array(Queue | Exchange) | Nil = nil)
      itr = if resources
              resources.each
            else
              @queues.each_value.chain(@exchanges.each_value)
            end
      sorted_policies = @policies.values.sort_by!(&.priority).reverse
      itr.each do |r|
        match = sorted_policies.find { |p| p.match?(r) }
        match.nil? ? r.clear_policy : r.apply_policy(match)
      end
    rescue ex : TypeCastError
      @log.warn { "Invalid policy. #{ex.message}" }
    end

    private def apply_parameters(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        case p.component_name
        when SHOVEL
          @shovels.not_nil!.create(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM
          @upstreams.not_nil!.create_upstream(p.parameter_name, p.value)
        when FEDERATION_UPSTREAM_SET
          @upstreams.not_nil!.create_upstream_set(p.parameter_name, p.value)
        else
          @log.warn { "No action when applying parameter #{p.component_name}" }
        end
      end
    end

    private def load!
      load_definitions!
      spawn(name: "Load parameters") do
        sleep 0.05
        apply_parameters
        apply_policies
      end
    end

    private def load_definitions!
      File.open(File.join(@data_dir, "definitions.amqp"), "r") do |io|
        io.advise(File::Advice::Sequential)
        loop do
          begin
            AMQP::Frame.from_io(io, IO::ByteFormat::NetworkEndian) do |frame|
              apply frame, loading: true
            end
          rescue ex : IO::EOFError
            break
          end
        end
      end
    rescue Errno
      load_default_definitions
    end

    private def load_default_definitions
      @log.info "Loading default definitions"
      @exchanges[""] = DirectExchange.new(self, "", true, false, false)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", true, false, false)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", true, false, false)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", true, false, false)
      @exchanges["amq.headers"] = HeadersExchange.new(self, "amq.headers", true, false, false)
      @exchanges["amq.match"] = HeadersExchange.new(self, "amq.match", true, false, false)
    end

    private def compact!
      @log.info "Compacting definitions"
      tmp_path = File.join(@data_dir, "definitions.amqp.tmp")
      File.open(tmp_path, "w") do |io|
        @exchanges.each do |_name, e|
          next unless e.durable
          next if e.auto_delete
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
                                                 false, e.durable, e.auto_delete, e.internal,
                                                 false, AMQP::Table.new(e.arguments))
          io.write_bytes f, ::IO::ByteFormat::NetworkEndian
        end
        @queues.each do |_name, q|
          next unless q.durable
          next if q.auto_delete # FIXME: Auto delete should be persistet, but also deleted
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
                                              q.auto_delete, false, AMQP::Table.new(q.arguments))
          io.write_bytes f, ::IO::ByteFormat::NetworkEndian
        end
        @exchanges.each do |_name, e|
          next unless e.durable
          next if e.auto_delete
          e.bindings.each do |bt, destinations|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            destinations.each do |d|
              f =
                case d
                when Queue
                  AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, d.name, e.name, bt[0], false, args)
                when Exchange
                  AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, e.name, d.name, bt[0], false, args)
                else raise "Unknown destination type #{d.class}"
                end
              io.write_bytes f, ::IO::ByteFormat::NetworkEndian
            end
          end
        end
        io.fsync
        io.advise(File::Advice::DontNeed)
      end
      File.rename tmp_path, File.join(@data_dir, "definitions.amqp")
    end

    private def save!
      return unless Dir.exists?(@data_dir)
      File.open(File.join(@data_dir, "definitions.amqp"), "a") do |f|
        loop do
          frame = @save.receive
          case frame
          when AMQP::Frame::Exchange::Declare
            next unless frame.durable
          when AMQP::Frame::Queue::Declare
            next unless frame.durable
            next if frame.exclusive
          when AMQP::Frame::Exchange::Delete
            next unless @exchanges[frame.exchange_name]?.try(&.durable)
          when AMQP::Frame::Queue::Delete
            next unless @queues[frame.queue_name]?.try { |q| q.durable && !q.exclusive }
          when AMQP::Frame::Queue::Bind, AMQP::Frame::Queue::Unbind
            next unless @exchanges[frame.exchange_name]?.try(&.durable)
            q = @queues[frame.queue_name]
            next if !q.durable || q.exclusive
          when AMQP::Frame::Exchange::Bind, AMQP::Frame::Exchange::Unbind
            s = @exchanges[frame.source]
            next unless s.durable
            d = @exchanges[frame.destination]
            next unless d.durable
          else raise "Cannot apply frame #{frame.class} in vhost #{@name}"
          end
          @log.debug { "Storing definition: #{frame.inspect}" }
          f.write_bytes frame, ::IO::ByteFormat::NetworkEndian
          f.fsync
          f.advise(File::Advice::DontNeed)
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

    @referenced_segments = Set(UInt32).new

    private def gc_segments!
      @log.info "Garbage collecting segments"
      @referenced_segments << @segment
      @queues.each_value do |q|
        q.referenced_segments(@referenced_segments)
      end
      @log.info "#{@referenced_segments.size} segments in use"

      Dir.each(@data_dir) do |f|
        if f.starts_with? "msgs."
          seg = f[5, 10].to_u32
          next if @referenced_segments.includes? seg
          @log.info "Deleting segment #{seg}"
          File.delete File.join(@data_dir, f)
        end
      end
      @referenced_segments.clear
    end
  end
end
