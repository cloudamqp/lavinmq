require "json"

module AMQPServer
  class VHost
    getter name, exchanges, queues

    def initialize(@name : String, @data_dir : String)
      load!
    end

    def declare_exchange(name, type, durable, arguments)
      @exchanges[name] = Exchange.new(name, type, durable, arguments)
      save! if durable
    end

    def load!
      File.open(File.join(@data_dir, @name, "exchanges.json.gz")) do |f|
        Gzip::Reader.open(f) do |gz|
          @exchanges = Hash(String, Exchange).from_json(gz)
        end
      end
      File.open(File.join(@data_dir, @name, "queues.json.gz")) do |f|
        Gzip::Reader.open(f) do |gz|
          @queues = Hash(String, Queue).from_json(gz)
        end
      end
    end

    def save!
      File.open(File.join(@data_dir, @name, "exchanges.json.gz"), "w") do |f|
        Gzip::Writer.open(f) do |gz|
          @exchanges.select { |e| e.durable }.to_json(gz)
        end
      end
      File.open(File.join(@data_dir, @name, "queues.json.gz"), "w") do |f|
        Gzip::Writer.open(f) do |gz|
          @queues.select { |e| e.durable }.to_json(gz)
        end
      end
    end

    def load_default_definitions
      @queues = {
        "q1" => Queue.new("q1", durable: true, auto_delete: false, exclusive: false, arguments: {} of String => AMQP::Field)
      }
      @exchanges = {
        "" => Exchange.new("", type: "direct", durable: true,
                           arguments: {} of String => AMQP::Field,
                           bindings: { "q1" => [@queues["q1"]] })
      }
    end
  end
end
