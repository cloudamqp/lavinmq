require "json"

module AMQPServer
  class VHost
    getter name, exchanges, queues

    def initialize(@name : String, @data_dir : String)
      @exchanges = Hash(String, Exchange).new
      @queues = Hash(String, Queue).new
      @save = Channel(AMQP::Frame).new
      load!
      compact!
      spawn save!
    end

    def save!
      File.open(File.join(@data_dir, @name, "definitions.amqp"), "a") do |f|
        loop do
          @save.receive.encode(f)
        end
      end
    end

    def apply(f : AMQP::Exchange::Declare)
      @save.send f
      @exchanges[f.exchange_name] =
        Exchange.new(f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
    end

    def apply(f : AMQP::Queue::Declare)
      @save.send f
      @queues[f.queue_name] =
        Queue.new(f.queue_name, f.durable, f.exclusive, f.auto_delete, f.arguments)
      @exchanges[""].bindings[f.queue_name] = [@queues[f.queue_name]]
    end

    def apply(f : AMQP::Queue::Bind)
      @save.send f
      @exchanges[f.exchange_name].bindings[f.queue_name] = [@vhost.queues[f.queue_name]]
    end

    def load!
      File.open(File.join(@data_dir, @name, "definitions.amqp"), "r") do |io|
        loop do
          begin
            f = AMQP::Frame.decode(io)
            case f
            when AMQP::Exchange::Declare
              @exchanges[f.exchange_name] =
                Exchange.new(f.exchange_name, f.exchange_type, f.durable, f.auto_delete, f.internal, f.arguments)
            when AMQP::Exchange::Delete
              @exchanges.delete f.exchange_name
            when AMQP::Queue::Declare
              @queues[f.queue_name] =
                Queue.new(f.queue_name, f.durable, f.auto_delete, f.exclusive, f.arguments)
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
      @queues = {
        "q1" => Queue.new("q1", durable: true, auto_delete: false, exclusive: false, arguments: {} of String => AMQP::Field)
      }
      @exchanges = {
        "" => Exchange.new("", type: "direct", durable: true,
                           auto_delete: false, internal: true,
                           arguments: {} of String => AMQP::Field,
                           bindings: { "q1" => [@queues["q1"]] })
      }
    end
  end
end
