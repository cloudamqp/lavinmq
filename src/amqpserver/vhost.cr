require "json"

module AMQPServer
  class VHost
    getter name, exchanges, queues

    def initialize(@name : String, @server_data_dir : String)
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Frame).new
      load!
      compact!
      spawn save!
    end

    def data_dir
      File.join(@server_data_dir, @name)
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
      when AMQP::Queue::Delete
        @queues.delete f.queue_name
        @exchanges.each do |_name, e|
          e.bindings.each do |_rk, queues|
            queues.delete f.queue_name
          end
        end
      when AMQP::Queue::Bind
        @exchanges[f.exchange_name].bind(f.queue_name, f.routing_key, f.arguments)
      when AMQP::Queue::Unbind
        @exchanges[f.exchange_name].unbind(f.queue_name, f.routing_key)
      else raise "Cannot apply frame #{f.class} in vhost #@name"
      end
    end

    def close
      @queues.each { |_, q| q.close }
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
      @exchanges[""] = DefaultExchange.new(self)
      @exchanges["amq.direct"] = DirectExchange.new(self, "amq.direct", "direct",
                                                    true, false, true)
      @exchanges["amq.fanout"] = FanoutExchange.new(self, "amq.fanout", "fanout",
                                                    true, false, true)
      @exchanges["amq.topic"] = TopicExchange.new(self, "amq.topic", "topic",
                                                  true, false, true)
    end

    private def compact!
      Dir.mkdir_p data_dir
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
    end
  end
end
