require "json"
require "logger"
require "../stdlib/*"
require "./segment_position"
require "./policy"
require "./parameter_store"
require "./parameter"
require "./shovel/shovel_store"
require "./federation/upstream_store"
require "./sortable_json"
require "./durable_queue"
require "./exchange/direct"
require "./exchange/default"
require "./exchange/topic"
require "./exchange/fanout"
require "./exchange/federation"
require "./exchange/consistent_hash"
require "./exchange/headers"
require "digest/sha1"
require "./reference_counter"
require "./mfile"
require "./queue_factory"
require "./schema"
require "./stats"

module AvalancheMQ
  class VHost
    include SortableJSON
    include Stats

    getter name, exchanges, queues, log, data_dir, policies, parameters,
      log, shovels, direct_reply_channels, upstreams, default_user,
      connections
    property? flow = true
    getter? closed = false

    @exchanges = Hash(String, Exchange).new
    @queues = Hash(String, Queue).new
    @save = Channel(AMQP::Frame).new(32)
    @write_lock = Mutex.new(:unchecked)
    @wfile : MFile
    @log : Logger
    @direct_reply_channels = Hash(String, Client::Channel).new
    @shovels : ShovelStore?
    @upstreams : Federation::UpstreamStore?
    @fsync = false
    @connections = Array(Client).new(512)
    @segments : Hash(UInt32, MFile)
    EXCHANGE_TYPES = %w(direct fanout topic headers
                        x-federation-upstream x-delayed-message
                        x-consistent-hash)

    rate_stats(%w(connection_created connection_closed queue_declared queue_deleted))

    def initialize(@name : String, @server_data_dir : String,
                   @log : Logger, @default_user : User)
      @log.progname = "vhost=#{@name}"
      @dir = Digest::SHA1.hexdigest(@name)
      @data_dir = File.join(@server_data_dir, @dir)
      Dir.mkdir_p File.join(@data_dir, "tmp")
      File.write(File.join(@data_dir, ".vhost"), @name)
      @segments = load_segments_on_disk
      @wfile = @segments.last_value
      @policies = ParameterStore(Policy).new(@data_dir, "policies.json", @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      @shovels = ShovelStore.new(self)
      @upstreams = Federation::UpstreamStore.new(self)
      load!
      compact!
      spawn save!, name: "VHost/#{@name}#save!"
      spawn gc_segments_loop, name: "VHost/#{@name}#gc_segments_loop"
    end

    def inspect(io : IO)
      io << "#<" << self.class << ": " << "@name=" << @name << ">"
    end

    @awaiting_confirm_lock = Mutex.new(:unchecked)
    @awaiting_confirm = Set(Client::Channel).new

    def waiting4confirm(channel)
      @awaiting_confirm_lock.synchronize do
        @awaiting_confirm.add channel
        unless @fsync
          @fsync = true
          spawn(name: "VHost/#{@name}#fsync") { fsync }
        end
      end
    end

    @queues_to_fsync_lock = Mutex.new(:unchecked)
    @queues_to_fsync = Set(DurableQueue).new

    def fsync
      return unless @fsync
      @log.debug { "fsync" }
      @wfile.fsync
      @queues_to_fsync_lock.synchronize do
        @queues_to_fsync.each &.fsync_enq
      end
      @awaiting_confirm_lock.synchronize do
        @awaiting_confirm.each do |ch|
          ch.confirm_ack(multiple: true)
        end
        @awaiting_confirm.clear
        @fsync = false
      end
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
                visited = Set(Exchange).new, found_queues = Set(Queue).new) : Bool
      ex = @exchanges[msg.exchange_name]? || return false
      ex.publish_in_count += 1
      properties = msg.properties
      headers = properties.headers
      find_all_queues(ex, msg.routing_key, headers, visited, found_queues)
      headers.try(&.delete("BCC"))
      prevent_dead_letter_loop(headers, found_queues)
      @log.debug { "publish queues#found=#{found_queues.size}" }
      if found_queues.empty?
        ex.unroutable_count += 1
        return false
      end
      return false if immediate && !found_queues.any? { |q| q.immediate_delivery? }
      sp = write_to_disk(msg, ex.persistent?)
      flush = properties.delivery_mode == 2_u8
      ok = 0
      found_queues.each do |q|
        if q.publish(sp, flush)
          ex.publish_out_count += 1
          if q.is_a?(DurableQueue) && flush
            @queues_to_fsync_lock.synchronize do
              @queues_to_fsync << q
            end
          end
          ok += 1
        end
      end
      ok > 0
    ensure
      visited.clear
      found_queues.clear
    end

    # drop msgs that aren't rejected and end up in the same queue
    private def prevent_dead_letter_loop(headers, found_queues)
      return if headers.nil? || found_queues.empty?
      if xdeaths = headers["x-death"]?.as?(Array(AMQ::Protocol::Field))
        xdeaths.each do |xd|
          if xd = xd.as?(AMQ::Protocol::Table)
            break if xd["reason"]? == "rejected"
            if xd_queue_name = xd["queue"]?
              if queue = found_queues.find { |q| q.name == xd_queue_name }
                @log.debug { "preventing dead letter loop on queue '#{queue.name}'" }
                found_queues.delete(queue)
              end
            end
          end
        end
      end
    end

    private def find_all_queues(ex : Exchange, routing_key : String,
                                headers : AMQP::Table?,
                                visited : Set(Exchange),
                                queues : Set(Queue)) : Nil
      ex.queue_matches(routing_key, headers) do |q|
        queues.add? q
      end

      ex.exchange_matches(routing_key, headers) do |e2e|
        if visited.add?(ex)
          find_all_queues(e2e, routing_key, headers, visited, queues)
        end
      end

      if cc = headers.try(&.fetch("CC", nil))
        if cc = cc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          cc.each do |rk|
            if rk = rk.as?(String)
              find_all_queues(ex, rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("CC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("CC header not a string array")
        end
      end

      if bcc = headers.try(&.fetch("BCC", nil))
        if bcc = bcc.as?(Array(AMQP::Field))
          hdrs = headers.not_nil!.clone
          hdrs.delete "CC"
          hdrs.delete "BCC"
          bcc.each do |rk|
            if rk = rk.as?(String)
              find_all_queues(ex, rk, hdrs, visited, queues)
            else
              raise Error::PreconditionFailed.new("BCC header not a string array")
            end
          end
        else
          raise Error::PreconditionFailed.new("BCC header not a string array")
        end
      end

      if queues.empty? && ex.alternate_exchange
        if ae = @exchanges[ex.alternate_exchange]?
          if visited.add?(ae)
            find_all_queues(ae, routing_key, headers, visited, queues)
          end
        end
      end
    end

    private def write_to_disk(msg, store_offset = false) : SegmentPosition
      if @wfile.capacity < @wfile.size + msg.bytesize
        open_new_segment(msg.bytesize)
      end
      wfile = @wfile
      @write_lock.synchronize do
        wfile.seek(0, IO::Seek::End)
        sp = SegmentPosition.make(@segments.last_key, wfile.pos.to_u32, msg)
        if store_offset
          headers = msg.properties.headers || AMQP::Table.new
          headers["x-offset"] = sp.to_i64
          msg.properties.headers = headers
        end
        @log.debug { "Writing message: exchange=#{msg.exchange_name} routing_key=#{msg.routing_key} \
                      size=#{msg.bytesize} sp=#{sp}" }
        wfile.write_bytes msg.timestamp
        wfile.write_bytes AMQP::ShortString.new(msg.exchange_name)
        wfile.write_bytes AMQP::ShortString.new(msg.routing_key)
        wfile.write_bytes msg.properties
        wfile.write_bytes msg.size
        copied = IO.copy(msg.body_io, wfile, msg.size)
        if copied != msg.size
          raise IO::Error.new("Could only write #{copied} of #{msg.size} bytes to message store")
        end
        wfile.flush if msg.persistent?
        sp
      end
    end

    private def open_new_segment(next_msg_size = 0) : MFile
      fsync
      next_id = @segments.empty? ? 1_u32 : @segments.last_key + 1
      @log.debug { "Opening message store segment #{next_id}" }
      filename = "msgs.#{next_id.to_s.rjust(10, '0')}"
      path = File.join(@data_dir, filename)
      capacity = Config.instance.segment_size + next_msg_size
      wfile = MFile.new(path, capacity)
      SchemaVersion.prefix(wfile)
      @wfile = @segments[next_id] = wfile
    end

    def segment_file(id : UInt32) : MFile
      @segments[id]
    end

    def details_tuple
      {
        name: @name,
        dir:  @dir,
      }
    end

    def message_details
      ready = unacked = 0
      @queues.each_value do |q|
        ready += q.message_count
        unacked += q.unacked_count
      end
      {
        messages:                ready + unacked,
        messages_unacknowledged: unacked,
        messages_ready:          ready,
      }
    end

    def declare_queue(name, durable, auto_delete, arguments = AMQP::Table.new)
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
          make_exchange(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments.to_h)
        apply_policies([e] of Exchange) unless loading
      when AMQP::Frame::Exchange::Delete
        if x = @exchanges.delete f.exchange_name
          @exchanges.each_value do |ex|
            ex.exchange_bindings.each do |binding_args, destinations|
              ex.unbind(x, *binding_args) if destinations.includes?(x)
            end
          end
          x.delete
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
        q = @queues[f.queue_name] = QueueFactory.make(self, f)
        @queue_declared_count += 1
        apply_policies([q] of Queue) unless loading
      when AMQP::Frame::Queue::Delete
        if q = @queues.delete(f.queue_name)
          @exchanges.each_value do |ex|
            ex.queue_bindings.each do |binding_args, destinations|
              ex.unbind(q, *binding_args) if destinations.includes?(q)
            end
          end
          @queue_deleted_count += 1
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
      @connections << client
      @connection_created_count += 1
      client.on_close do |c|
        @connections.delete c
        @connection_closed_count += 1
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
      @shovels.not_nil!.each_value &.terminate
    end

    def stop_upstream_links
      @upstreams.not_nil!.stop_all
    end

    def close
      @closed = true
      @log.info("Closing")
      stop_shovels
      Fiber.yield
      stop_upstream_links
      Fiber.yield
      @log.debug "Closing connections"
      @connections.each &.close("Broker shutdown")
      # wait up to 10s for clients to gracefully close
      100.times do
        break if @connections.empty?
        sleep 0.1
      end
      @queues.each_value &.close
      Fiber.yield
      @save.close
      Fiber.yield
      @segments.each_value &.close
      compact!
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
        io.buffer_size = Config.instance.file_buffer_size
        io.advise(File::Advice::Sequential)
        SchemaVersion.verify(io)
        loop do
          begin
            AMQP::Frame.from_io(io, IO::ByteFormat::SystemEndian) do |frame|
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
    rescue IO::Error
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
        io.buffer_size = Config.instance.file_buffer_size
        SchemaVersion.prefix io
        @exchanges.each do |_name, e|
          next unless e.durable
          f = AMQP::Frame::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
            false, e.durable, e.auto_delete, e.internal,
            false, AMQP::Table.new(e.arguments))
          io.write_bytes f
        end
        @queues.each do |_name, q|
          next unless q.durable
          f = AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
            q.auto_delete, false, AMQP::Table.new(q.arguments))
          io.write_bytes f
        end
        @exchanges.each do |_name, e|
          next unless e.durable
          e.queue_bindings.each do |bt, queues|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            queues.each do |q|
              f = AMQP::Frame::Queue::Bind.new(0_u16, 0_u16, q.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
          e.exchange_bindings.each do |bt, exchanges|
            args = AMQP::Table.new(bt[1]) || AMQP::Table.new
            exchanges.each do |ex|
              f = AMQP::Frame::Exchange::Bind.new(0_u16, 0_u16, ex.name, e.name, bt[0], false, args)
              io.write_bytes f
            end
          end
        end
        io.fsync
      end
      File.rename tmp_path, File.join(@data_dir, "definitions.amqp")
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def save!
      File.open(File.join(@data_dir, "definitions.amqp"), "W") do |f|
        f.seek(0, IO::Seek::End)
        while frame = @save.receive?
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
          f.write_bytes frame
          f.fsync
        end
      end
      @policies.save!
    rescue ex
      abort "ERROR: Writing definitions. #{ex.inspect}"
    end

    private def load_segments_on_disk
      ids = Array(UInt32).new
      Dir.each_child(@data_dir) do |f|
        if f.starts_with? "msgs."
          ids << f[5, 10].to_u32
        end
      end
      ids.sort!
      was_empty = ids.empty?
      ids << 1_u32 if was_empty
      segments = Hash(UInt32, MFile).new(initial_capacity: Math.pw2ceil(Math.max(ids.size, 16)))
      last_idx = ids.size - 1
      ids.each_with_index do |seg, idx|
        filename = "msgs.#{seg.to_s.rjust(10, '0')}"
        path = File.join(@data_dir, filename)
        file = if idx == last_idx
                 # expand the last segment
                 MFile.new(path, Config.instance.segment_size)
               else
                 MFile.new(path)
               end
        if was_empty
          SchemaVersion.prefix(file)
        else
          SchemaVersion.verify(file)
        end
        segments[seg] = file
      end
      segments
    end

    @referenced_sps = Array(SegmentPosition).new(1_000_000)

    private def gc_segments_loop
      # Don't gc all vhosts at the same time
      sleep Random.rand(Config.instance.gc_segments_interval)
      loop do
        sleep Config.instance.gc_segments_interval
        break if @closed
        elapsed = Time.measure do
          collect_sps
        end
        gc_log("collecting sps", elapsed)
        elapsed = Time.measure do
          delete_unused_segments
        end
        gc_log("delete unused segs", elapsed)
        elapsed = Time.measure do
          hole_punch_segments
        end
        gc_log("hole punching", elapsed)

        # If less than half the capacity is used, recreate to reclaim RAM
        current_capacity = @referenced_sps.capacity
        new_capacity = Math.max(1_000_000, @referenced_sps.size)
        if (new_capacity < current_capacity) && (current_capacity > @referenced_sps.size * 2)
          @log.debug { "Reclaim sp reference memory from #{current_capacity} to #{new_capacity}" }
          @referenced_sps = Array(SegmentPosition).new(new_capacity)
          GC.collect
        else
          @referenced_sps.clear
        end
      end
    end

    private def gc_log(task, elapsed)
      return if elapsed.total_milliseconds <= 10
      @log.info { "GC segments, #{task} took #{elapsed.total_milliseconds} ms" }
    end

    private def collect_sps : Nil
      @exchanges.each_value do |ex|
        ex.referenced_sps(@referenced_sps)
      end
      @queues.each_value do |q|
        q.unacked.copy_to @referenced_sps
        q.ready.copy_to @referenced_sps
      end
      @referenced_sps.sort!
    end

    private def delete_unused_segments : Nil
      @log.debug "Garbage collecting segments"
      deleted_bytes = 0_u64
      @segments.delete_if do |seg, mfile|
        next if seg == @segments.last_key
        sp = @referenced_sps.bsearch { |x| x.segment >= seg }
        if sp.try(&.segment) != seg
          @log.debug { "Deleting segment #{seg}" }
          deleted_bytes += mfile.size
          mfile.delete
          mfile.close
          true
        end
      end
      @log.info { "Garbage collected #{deleted_bytes.humanize_bytes} of unused segments" } if deleted_bytes > 0
      @log.debug { "#{@segments.size} segments on disk" }
    end

    private def hole_punch_segments : Nil
      @log.debug { "Hole punching segments" }

      punched = 0_u64
      file = nil
      prev_sp = SegmentPosition.zero
      prev_sp_end = sizeof(Int32).to_u32 # start after schema version prefix
      @referenced_sps.each do |sp|
        next if sp == prev_sp # ignore duplicates

        # if the last segment was the same as this
        if prev_sp.segment == sp.segment
          # if there's a hole between previous sp and this sp
          # punch a hole
          if file.nil?
            @log.info { "prev_sp: #{prev_sp} sp: #{sp.inspect}, file nil" }
          else
            punched += punch_hole(file.not_nil!, prev_sp_end, sp.position)
          end
        else # dealing with a new segment
          # punch to the end of the last file
          if seg = file
            punched += punch_hole(seg, prev_sp_end, seg.size)
            seg.close
          end

          # then open this segment
          path = File.join(@data_dir, "msgs.#{sp.segment.to_s.rjust(10, '0')}")
          file = File.open(path, "a+").tap do |f|
            f.buffer_size = Config.instance.file_buffer_size
          end
          @log.debug { "GC seg, open #{sp.segment}, size: #{file.size}" }

          # punch from start of the segment (but not the version prefix)
          punched += punch_hole(file, sizeof(Int32), sp.position)
        end
        prev_sp_end = sp.position + sp.bytesize
        prev_sp = sp
      end

      if file && prev_sp.segment != @segments.last_key
        punched += punch_hole(file, prev_sp_end, file.size)
        file.close
      end

      @log.info { "Garbage collected #{punched.humanize_bytes} by hole punching" } if punched > 0
    end

    private def punch_hole(segment : File, start_pos : Int, end_pos : Int)
      hole_size = end_pos - start_pos
      if hole_size > 0
        @log.debug { "Punch hole in #{segment.path}, from #{start_pos}, to #{end_pos}" }
        segment.punch_hole(hole_size, start_pos)
        hole_size
      else
        0
      end
    end

    private def make_exchange(vhost, name, type, durable, auto_delete, internal, arguments)
      case type
      when "direct"
        DirectExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "fanout"
        FanoutExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "topic"
        TopicExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "headers"
        HeadersExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      when "x-delayed-message"
        type = arguments.delete("x-delayed-type")
        raise "Missing required argument 'x-delayed-type'" unless type
        arguments["x-delayed-message"] = true
        make(vhost, name, type, durable, auto_delete, internal, arguments)
      when "x-federation-upstream"
        FederationExchange.new(vhost, name, arguments)
      when "x-consistent-hash"
        ConsistentHashExchange.new(vhost, name, durable, auto_delete, internal, arguments)
      else raise "Cannot make exchange type #{type}"
      end
    end
  end
end
