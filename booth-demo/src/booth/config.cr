require "yaml"

module Booth
  class Config
    include YAML::Serializable

    class Node
      include YAML::Serializable

      getter name : String
      getter host : String

      def initialize(@name, @host)
      end
    end

    class WorkQueue
      include YAML::Serializable
      property? enabled = true
      property rate = 500 # msgs/s across all publishers
      property publishers = 2
      property consumers = 4
      property size = 256

      def initialize
      end
    end

    class Sequence
      include YAML::Serializable
      property? enabled = true
      property rate = 100

      def initialize
      end
    end

    class Stream
      include YAML::Serializable
      property? enabled = true
      property rate = 100
      property replay_rate = 300 # msgs/s for the consumer replaying from the start

      def initialize
      end
    end

    class Iot
      include YAML::Serializable
      property? enabled = true
      property devices = 300
      property interval = 2.0 # seconds between readings per device
      property max_devices = 2000

      def initialize
      end
    end

    class Scenarios
      include YAML::Serializable
      getter work_queue = WorkQueue.new
      getter sequence = Sequence.new
      getter stream = Stream.new
      getter iot = Iot.new

      def initialize
      end
    end

    getter nodes = [] of Node
    getter user = "booth"
    getter password = "booth"
    getter vhost = "/"
    getter amqp_port = 5672
    getter mqtt_port = 1883
    getter http_port = 15672
    getter metrics_port = 15692
    getter listen = "0.0.0.0:8080"
    getter title = "LavinMQ on Raspberry Pi"
    getter scenarios = Scenarios.new

    def initialize(@nodes)
    end

    def self.load(path : String) : Config
      config = File.open(path) { |f| from_yaml(f) }
      raise ArgumentError.new("#{path}: at least one node required") if config.nodes.empty?
      config
    end

    def listen_host : String
      listen.rpartition(':')[0]
    end

    def listen_port : Int32
      listen.rpartition(':')[2].to_i
    end
  end
end
