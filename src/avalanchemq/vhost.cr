require "json"
require "logger"
require "./amqp/io"
require "./segment_position"
require "digest/sha1"

module AvalancheMQ
  class VHost
    class MessageFile < File
      include AMQP::IO
    end
    getter name, exchanges, queues, log

    MAX_SEGMENT_SIZE = 256 * 1024**2
    @segment : UInt32
    @log : Logger

    def initialize(@name : String, @server_data_dir : String, server_log : Logger)
      @log = server_log.dup
      @log.progname = "Vhost #{@name}"
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Frame).new(16)
      Dir.mkdir_p data_dir
      @segment = last_segment
      @wfile = MessageFile.open(File.join(data_dir, "msgs.#{@segment}"), "a")
      @wfile.seek(0, IO::Seek::End)
      load!
      compact!
      spawn save!, name: "VHost#save!"
    end

    def publish(msg : Message)
      ex = @exchanges[msg.exchange_name]?
      return if ex.nil?
      queues = ex.queues_matching(msg.routing_key)
      return if queues.empty?

      pos = @wfile.pos.to_u32
      sp = SegmentPosition.new(@segment, pos)
      @wfile.write_int msg.timestamp
      @wfile.write_short_string msg.exchange_name
      @wfile.write_short_string msg.routing_key
      @wfile.write_bytes msg.properties
      @wfile.write_int msg.size
      @wfile.write msg.body.to_slice
      flush = true #msg.properties.delivery_mode.try { |v| v > 0 }
      @wfile.flush if flush
      queues.each { |q| @queues.fetch(q, nil).try &.publish(sp, flush) }

      if @wfile.pos >= MAX_SEGMENT_SIZE
        @segment += 1
        @wfile.close
        @wfile = MessageFile.open(File.join(data_dir, "msgs.#{@segment}"), "a")
        spawn gc_segments!
      end
    end

    def data_dir
      File.join(@server_data_dir, Digest::SHA1.hexdigest(@name))
    end

    def apply(f, loading = false)
      @save.send f unless loading
      case f
      when AMQP::Exchange::Declare
        @exchanges[f.exchange_name] =
          Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
      when AMQP::Exchange::Delete
        @exchanges.delete f.exchange_name
      when AMQP::Queue::Declare
        @queues[f.queue_name] =
          Queue.new(self, f.queue_name, f.durable, f.exclusive, f.auto_delete, f.arguments)
        @exchanges[""].bind(f.queue_name, f.queue_name)
      when AMQP::Queue::Delete
        @exchanges.each_value do |e|
          e.bindings.each_value do |queues|
            queues.delete f.queue_name
          end
        end
        @queues.delete(f.queue_name).try { |q| q.delete }
      when AMQP::Queue::Bind
        @exchanges[f.exchange_name].bind(f.queue_name, f.routing_key, f.arguments)
      when AMQP::Queue::Unbind
        @exchanges[f.exchange_name].unbind(f.queue_name, f.routing_key)
      else raise "Cannot apply frame #{f.class} in vhost #{@name}"
      end
    end

    def close
      @save.close
      @queues.each_value &.close
    end

    private def load!
      File.open(File.join(data_dir, "definitions.amqp"), "r") do |io|
        loop do
          begin
            apply AMQP::Frame.decode(io), loading: true
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
      @exchanges[""] = DirectExchange.new(self, "", true, false, true)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct",
                                                    true, false, true)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout",
                                                    true, false, true)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic",
                                                  true, false, true)
    end

    private def compact!
      File.open(File.join(data_dir, "definitions.amqp"), "w") do |io|
        @exchanges.each do |name, e|
          next unless e.durable
          next if e.auto_delete
          f = AMQP::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
                                          false, e.durable, e.auto_delete, e.internal,
                                          false, e.arguments)
          f.encode(io)
          e.bindings.each do |rk, queues|
            queues.each do |q|
              f = AMQP::Queue::Bind.new(0_u16, 0_u16, q, e.name, rk, false, Hash(String, AMQP::Field).new)
              f.encode(io)
            end
          end
        end
        @queues.each do |name, q|
          next unless q.durable
          next if q.auto_delete
          f = AMQP::Queue::Declare.new(0_u16, 0_u16, q.name, false, q.durable, q.exclusive,
                                       q.auto_delete, false, q.arguments)
          f.encode(io)
        end
      end
    end

    private def save!
      File.open(File.join(data_dir, "definitions.amqp"), "a") do |f|
        loop do
          frame = @save.receive
          case frame
          when AMQP::Exchange::Declare, AMQP::Queue::Declare
            next if !frame.durable || frame.auto_delete
          when AMQP::Queue::Bind, AMQP::Queue::Unbind
            e = @exchanges[frame.exchange_name]
            next if !e.durable || e.auto_delete
            q = @queues[frame.queue_name]
            next if !q.durable || q.auto_delete
          end
          frame.encode(f)
          f.flush
        end
      end
    rescue Channel::ClosedError
      @log.debug "Save channel closed"
    end

    private def last_segment : UInt32
      last_segment = Dir.glob(File.join(data_dir, "msgs.*")).last { nil }
      return 0_u32 if last_segment.nil?
      last_segment[/\d+$/].to_u32
    end

    private def gc_segments!
      @log.info "Garbage collecting segments"
      referenced_segments = Set(UInt32).new([@segment])
      @queues.each_value do |q|
        used = q.close_unused_segments_and_report_used
        referenced_segments.concat used
      end
      @log.info "#{referenced_segments.size} segments in use"

      Dir.glob(File.join(data_dir, "msgs.*")).each do |f|
        seg = f[/\d+$/].to_u32
        next if referenced_segments.includes? seg
        @log.info "Deleting segment #{seg}"
        File.delete f
      end
    end
  end
end
