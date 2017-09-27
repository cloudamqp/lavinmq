require "json"

module AMQPServer
  class VHost
    getter name, exchanges, queues

    def initialize(@name : String, @data_dir : String)
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Exchange::Declare | AMQP::Queue::Declare | AMQP::Queue::Bind).new
      load!
      compact!
      spawn save!
    end

    def save!
      File.open(File.join(@data_dir, @name, "definitions.amqp"), "a") do |f|
        loop do
          frame = @save.receive
          case frame
          when AMQP::Exchange::Declare, AMQP::Queue::Declare
            next if !frame.durable || frame.auto_delete
          when AMQP::Queue::Bind
            e = @exchanges[frame.exchange_name]
            next if !e.durable || e.auto_delete
            q = @queues[frame.queue_name]
            next if !q.durable || q.auto_delete
          end
          frame.encode(f)
          f.flush
        end
      end
    end

    def apply(f : AMQP::Exchange::Declare)
      @save.send f
      @exchanges[f.exchange_name] =
        Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
    end

    def apply(f : AMQP::Queue::Declare)
      @save.send f
      @queues[f.queue_name] =
        Queue.new(self, f.queue_name, f.durable, f.exclusive, f.auto_delete, f.arguments)
    end

    def apply(f : AMQP::Queue::Bind)
      @save.send f
      @exchanges[f.exchange_name].bindings[f.routing_key] << f.queue_name
    end

    def load!
      File.open(File.join(@data_dir, @name, "definitions.amqp"), "r") do |io|
        loop do
          begin
            f = AMQP::Frame.decode(io)
            case f
            when AMQP::Exchange::Declare
              @exchanges[f.exchange_name] =
                Exchange.make(self, f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
            when AMQP::Exchange::Delete
              @exchanges.delete f.exchange_name
            when AMQP::Queue::Declare
              @queues[f.queue_name] =
                Queue.new(self, f.queue_name, f.durable, f.auto_delete, f.exclusive, f.arguments)
            when AMQP::Queue::Delete
              @queues.delete f.queue_name
            end
          rescue ex : IO::EOFError
            break
          end
        end
      end
    rescue Errno
      load_default_definitions
    end

    def compact!
      Dir.mkdir_p File.join(@data_dir, @name)
      File.open(File.join(@data_dir, @name, "definitions.amqp"), "w") do |io|
        @exchanges.each do |name, e|
          next unless e.durable
          next if e.auto_delete
          f = AMQP::Exchange::Declare.new(0_u16, 0_u16, e.name, e.type,
                                          false, e.durable, e.auto_delete, e.internal, false, e.arguments)
          f.encode(io)
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

    def load_default_definitions
      @exchanges[""] = DefaultExchange.new(self)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", "direct",
                                                    true, false, true)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", "fanout",
                                                    true, false, true)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", "topic",
                                                  true, false, true)
    end
  end
end
