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
    getter? closed = false

    @exchanges = Hash(String, Exchange).new
    @queues = Hash(String, Queue).new
    @save = Channel(AMQP::Frame).new(32)
    @segment : UInt32
    @wfile : File
    @segments_on_disk : Deque(UInt32)
    @log : Logger
    @direct_reply_channels = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    @write_lock = Mutex.new(:unchecked)
    @fsync = false
    EXCHANGE_TYPES = %w(direct fanout topic headers x-federation-upstream)

    def initialize(@name : String, @server_data_dir : String,
                   @log : Logger, @default_user : User,
                   @connection_events = Server::ConnectionsEvents.new(16))
      @log.progname = "vhost=#{@name}"
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir, "tmp")
      File.write(File.join(@data_dir, ".vhost"), @name)
      @segments_on_disk = load_segments_on_disk!
      @segment = @segments_on_disk.last
      @wfile = open_wfile
      @wfile.seek(0, IO::Seek::End)
      @pos = @wfile.pos.to_u32
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
      compact!
      spawn save!, name: "VHost/#{@name}#save!"
      spawn fsync_loop, name: "VHost/#{@name}#fsync_loop"
      spawn gc_segments_loop, name: "VHost/#{@name}#gc_segments_loop"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << ">"
    end

    @awaiting_confirm_lock = Mutex.new(:unchecked)
    @awaiting_confirm = Set(Client::Channel).new

    def waiting4confirm(channel)
      @fsync = true
      @awaiting_confirm_lock.synchronize do
        @awaiting_confirm.add channel
      end
    end

    private def fsync_loop
      loop do
        sleep 0.2
        break if @closed
        fsync
      end
    end

    @queues_to_fsync_lock = Mutex.new(:unchecked)
    @queues_to_fsync = Set(DurableQueue).new

    def fsync
      return unless @fsync
      @log.debug { "fsync" }
      @wfile.fsync(flush_metadata: false)
      @queues_to_fsync_lock.synchronize do
        @queues_to_fsync.each &.fsync_enq
      end
      @awaiting_confirm_lock.synchronize do
        @awaiting_confirm.each do |ch|
          ch.confirm_ack(multiple: true)
        end
        @awaiting_confirm.clear
      end
      @fsync = false
    end

    # Queue#publish can raise RejectPublish which should trigger a Nack. All other confirm scenarios
    # should be Acks, apart from Exceptions.
    # As long as at least one queue reject the publish due to overflow a Nack should be sent,
    # even if other queues accepts the message. Behaviour confirmed with RabbitMQ.
    # Method returns nil if the message hasn't been written to disk
    # True if it also succesfully wrote to one or more queues
    # False if no queue was able to receive the message because they're
    # closed
    def publish(msg : Message, immediate = false,
                visited = Set(Exchange).new, found_queues = Set(Queue).new) : Bool?
      ex = @exchanges[msg.exchange_name]? || return
      ex.publish_in_count += 1
      find_all_queues(ex, msg.routing_key, msg.properties.headers, visited, found_queues)
      @log.debug { "publish queues#found=#{found_queues.size}" }
      return if found_queues.empty?
      return if immediate && !found_queues.any? { |q| q.immediate_delivery? }
      sp = @write_lock.synchronize do
        write_to_disk(msg)
      end
      flush = msg.properties.delivery_mode == 2_u8
      ok = false
      found_queues.each do |q|
        if q.publish(sp, flush)
          ex.publish_out_count += 1
          if q.is_a?(DurableQueue) && flush
            @queues_to_fsync_lock.synchronize do
              @queues_to_fsync << q
            end
          end
          ok = true
        end
      end
      ok
    ensure
      visited.clear
      found_queues.clear
    end

    private def find_all_queues(ex : Exchange, routing_key : String,
                                headers : AMQP::Table?,
                                visited : Set(Exchange),
                                queues : Set(Queue)) : Nil
      ex.queue_matches(routing_key, headers) { |q| queues << q }

      visited.add(ex)
      ex.exchange_matches(routing_key, headers) do |e2e|
        unless visited.includes? e2e
          find_all_queues(e2e, routing_key, headers, visited, queues)
        end
      end

      if cc = headers.try(&.fetch("CC", nil))
        cc.as(Array(AMQP::Field)).each do |rk|
          find_all_queues(ex, rk.as(String), nil, visited, queues)
        end
      end

      if bcc = headers.try(&.delete("BCC"))
        bcc.as(Array(AMQP::Field)).each do |rk|
          find_all_queues(ex, rk.as(String), nil, visited, queues)
        end
      end

      if queues.empty? && ex.alternate_exchange
        if ae = @exchanges[ex.alternate_exchange]?
          unless visited.includes?(ae)
            find_all_queues(ae, routing_key, headers, visited, queues)
          end
        end
      end
    end

    private def write_to_disk(msg) : SegmentPosition
      if @pos >= Config.instance.segment_size
        open_new_segment
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
      begin
        @wfile.flush
        @pos = @wfile.pos.to_u32
      rescue
        open_new_segment
      end
      raise ex
    end

    private def open_new_segment
      @segment += 1
      @segments_on_disk << @segment
      fsync
      @wfile.close
      @wfile = open_wfile
    end

    private def open_wfile : File
      @log.debug { "Opening message store segment #{@segment}" }
      filename = "msgs.#{@segment.to_s.rjust(10, '0')}"
      File.open(File.join(@data_dir, filename), "a").tap do |f|
        f.buffer_size = Config.instance.file_buffer_size
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

    @apply_count = 0_u64

    # ameba:disable Metrics/CyclomaticComplexity
    def apply(f, loading = false) : Bool
      Fiber.yield if (@apply_count += 1) % 128 == 0
      case f
      when AMQP::Frame::Exchange::Declare
        return false if @exchanges.has_key? f.exchange_name
        e = @exchanges[f.exchange_name] =
          Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments.to_h)
        apply_policies([e] of Exchange) unless loading
      when AMQP::Frame::Exchange::Delete
        return false unless @exchanges.has_key? f.exchange_name
        if x = @exchanges.delete f.exchange_name
          @exchanges.each_value do |ex|
            ex.exchange_bindings.each_value do |destination|
              destination.delete x
            end
          end
        else
          return false
        end
      when AMQP::Frame::Exchange::Bind
        source = @exchanges[f.source]? || return false
        x = @exchanges[f.destination]? || return false
        source.bind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Exchange::Unbind
        source = @exchanges[f.source]? || return false
        x = @exchanges[f.destination]? || return false
        source.unbind(x, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Declare
        return false if @queues.has_key? f.queue_name
        q = @queues[f.queue_name] =
          if f.durable
            DurableQueue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments.to_h)
          else
            Queue.new(self, f.queue_name, f.exclusive, f.auto_delete, f.arguments.to_h)
          end
        apply_policies([q] of Queue) unless loading
      when AMQP::Frame::Queue::Delete
        if q = @queues.delete(f.queue_name)
          @exchanges.each_value do |ex|
            ex.queue_bindings.each_value do |destinations|
              destinations.delete q
            end
          end
          q.delete
        else
          return false
        end
      when AMQP::Frame::Queue::Bind
        x = @exchanges[f.exchange_name]? || return false
        q = @queues[f.queue_name]? || return false
        x.bind(q, f.routing_key, f.arguments.to_h)
      when AMQP::Frame::Queue::Unbind
        x = @exchanges[f.exchange_name]? || return false
        q = @queues[f.queue_name]? || return false
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
      @closed = true
      @log.info("Closing")
      @write_lock.synchronize do
        stop_shovels
        Fiber.yield
        stop_upstream_links
        Fiber.yield
        @queues.each_value &.close
        Fiber.yield
        @save.close
        Fiber.yield
        compact!
        @wfile.close
      end
      GC.collect
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
      # In 0.8.4 and older amq.default was a DirectExchange
      unless @exchanges[""].is_a? DefaultExchange
        @exchanges[""] = DefaultExchange.new(self, "", true, false, false)
      end
    rescue Errno
      load_default_definitions
    end

    private def load_default_definitions
      @log.info "Loading default definitions"
      @exchanges[""] = DefaultExchange.new(self, "", true, false, false)
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
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable, e.auto_delete, e.internal,
            false, AMQP::Table.new(e.arguments))
          io.write_bytes f, ::IO::ByteFormat::NetworkEndian
        end
        @queues.each do |_name, q|
          next unless q.durable
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
            q.auto_delete, false, AMQP::Table.new(q.arguments))
          io.write_bytes f, ::IO::ByteFormat::NetworkEndian
        end
        @exchanges.each do |_name, e|
          next unless e.durable
          e.queue_bindings.each do |bt, queues|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            queues.each do |q|
              f = AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, q.name, e.name, bt[0], false, args)
              io.write_bytes f, ::IO::ByteFormat::NetworkEndian
            end
          end
          e.exchange_bindings.each do |bt, exchanges|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            exchanges.each do |ex|
              f = AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, ex.name, e.name, bt[0], false, args)
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
          frame = @save.receive? || break
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
            next unless @exchanges[frame.exchange_name]?.try &.durable
            next unless @queues[frame.queue_name]?.try &.durable
          when AMQP::Frame::Exchange::Bind, AMQP::Frame::Exchange::Unbind
            next unless @exchanges[frame.source]?.try &.durable
            next unless @exchanges[frame.destination]?.try &.durable
          else raise "Cannot apply frame #{frame.class} in vhost #{@name}"
          end
          @log.debug { "Storing definition: #{frame.inspect}" }
          f.write_bytes frame, ::IO::ByteFormat::NetworkEndian
          f.fsync
          f.advise(File::Advice::DontNeed)
        end
      end
      @policies.save!
    ensure
      @save.close
    end

    private def load_segments_on_disk!
      segments = Array(UInt32).new
      Dir.each_child(@data_dir) do |f|
        if f.starts_with? "msgs."
          segments << f[5, 10].to_u32
        end
      end
      segments.sort!
      segments << 0_u32 if segments.empty?
      Deque(UInt32).new(segments)
    end

    private def gc_segments_loop
      referenced_segments = Set(UInt32).new
      loop do
        sleep Config.instance.gc_segments_interval
        break if @closed
        @log.debug "Garbage collecting segments"
        referenced_segments << @segment
        @queues.each_value do |q|
          q.referenced_segments(referenced_segments)
        end
        @log.debug "#{referenced_segments.size} segments in use"

        @segments_on_disk.delete_if do |seg|
          unless referenced_segments.includes? seg
            @log.debug "Deleting segment #{seg}"
            filename = "msgs.#{seg.to_s.rjust(10, '0')}"
            File.delete File.join(@data_dir, filename)
            true
          end
        end
        @log.debug "#{@segments_on_disk.size} segments on disk"
        referenced_segments.clear
      end
    end
  end
end
