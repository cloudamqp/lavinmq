require "../lavinmq/version"
require "../stdlib/slice"
require "json"
require "amq-protocol"

module LavinMQCtl
  class DefinitionsGenerator
    def initialize(@data_dir : String)
      {"vhosts.json", "users.json"}.each do |f|
        abort "#{f} not found. Is #{@data_dir} a data directory?" unless File.exists?(File.join(@data_dir, f))
      end
    end

    def generate(io)
      Dir.cd(@data_dir) do
        JSON.build(io, 2) do |json|
          json.object do
            json.field("lavinmq_version", LavinMQ::VERSION)
            json.field("vhosts") do
              json.array do
                vhosts.each do |v|
                  json.object do
                    json.field "name", v["name"]
                  end
                end
              end
            end
            json.field("users") do
              json.array do
                users.each do |u|
                  json.object do
                    json.field "name", u["name"]
                    json.field "password_hash", u["password_hash"]
                    json.field "hashing_algorithm", u["hashing_algorithm"]
                    json.field "tags", u["tags"]
                  end
                end
              end
            end
            json.field("permissions") do
              json.array do
                users.each do |u|
                  u["permissions"].as_h.each do |vhost, p|
                    json.object do
                      json.field "name", u["name"]
                      json.field "vhost", vhost
                      json.field "configure", p["config"]
                      json.field "read", p["read"]
                      json.field "write", p["write"]
                    end
                  end
                end
              end
            end
            json.field("exchanges") do
              json.array do
                each_vhost do |vhost, vhost_dir|
                  exchanges(vhost_dir).each do |e|
                    json.object do
                      json.field "vhost", vhost
                      json.field "name", e.exchange_name
                      json.field "durable", e.durable
                      json.field "auto_delete", e.auto_delete
                      json.field "internal", e.internal
                      json.field "arguments", e.arguments
                    end
                  end
                end
              end
            end
            json.field("queues") do
              json.array do
                each_vhost do |vhost, vhost_dir|
                  queues(vhost_dir).each do |q|
                    json.object do
                      json.field "vhost", vhost
                      json.field "name", q.queue_name
                      json.field "durable", q.durable
                      json.field "auto_delete", q.auto_delete
                      json.field "arguments", q.arguments
                    end
                  end
                end
              end
            end
            json.field("bindings") do
              json.array do
                each_vhost do |vhost, vhost_dir|
                  queue_bindings(vhost_dir).each do |b|
                    json.object do
                      json.field "vhost", vhost
                      json.field "source", b.exchange_name
                      json.field "destination", b.queue_name
                      json.field "destination_type", "queue"
                      json.field "routing_key", b.routing_key
                      json.field "arguments", b.arguments
                    end
                  end
                  exchange_bindings(vhost_dir).each do |b|
                    json.object do
                      json.field "vhost", vhost
                      json.field "source", b.source
                      json.field "destination", b.destination
                      json.field "destination_type", "exchange"
                      json.field "routing_key", b.routing_key
                      json.field "arguments", b.arguments
                    end
                  end
                end
              end
            end
            json.field("policies") do
              json.array do
                each_vhost do |_vhost, vhost_dir|
                  policies(vhost_dir).each do |p|
                    p.to_json(json)
                  end
                end
              end
            end
            json.field("parameters") do
              json.array do
                each_vhost do |vhost, vhost_dir|
                  parameters(vhost_dir).each do |p|
                    p.as_h.merge!({"vhost" => JSON::Any.new(vhost)}).to_json(json)
                  end
                  operator_policies(vhost_dir).each do |op|
                    {
                      "name"      => op["name"],
                      "vhost"     => vhost,
                      "component" => "operator_policy",
                      "value"     => op.as_h.reject!("name", "vhost"),
                    }.to_json(json)
                  end
                end
              end
            end
            json.field("global_parameters") do
              json.array { }
            end
          end
        end
      end
    end

    private def policies(vhost_dir)
      File.open(File.join(vhost_dir, "policies.json")) do |f|
        JSON.parse(f).as_a
      end
    rescue File::NotFoundError
      Array(JSON::Any).new
    end

    private def operator_policies(vhost_dir)
      File.open(File.join(vhost_dir, "operator_policies.json")) do |f|
        JSON.parse(f).as_a
      end
    rescue File::NotFoundError
      Array(JSON::Any).new
    end

    private def parameters(vhost_dir)
      File.open(File.join(vhost_dir, "parameters.json")) do |f|
        JSON.parse(f).as_a
      end
    rescue File::NotFoundError
      Array(JSON::Any).new
    end

    private def users
      File.open("users.json") { |f| JSON.parse f }.as_a
    end

    private def vhosts
      File.open("vhosts.json") { |f| JSON.parse(f) }.as_a
    end

    private def each_vhost(&)
      vhosts.each do |vhost|
        yield vhost["name"].as_s, vhost["dir"].as_s
      end
    end

    alias Frame = AMQ::Protocol::Frame

    private def queues(vhost_dir)
      queues = Array(Frame::Method::Queue::Declare).new
      File.open(File.join(vhost_dir, "definitions.amqp")) do |defs|
        _schema = defs.read_bytes Int32
        loop do
          frame = Frame.from_io(defs, IO::ByteFormat::SystemEndian) { |f| f }
          case frame
          when Frame::Method::Queue::Declare
            queues << frame
          when Frame::Method::Queue::Delete
            queues.reject! { |q| q.queue_name == frame.queue_name }
          end
        rescue IO::EOFError
          break
        end
      end
      queues
    end

    private def exchanges(vhost_dir)
      exchanges = Array(Frame::Method::Exchange::Declare).new
      File.open(File.join(vhost_dir, "definitions.amqp")) do |defs|
        _schema = defs.read_bytes Int32
        loop do
          frame = Frame.from_io(defs, IO::ByteFormat::SystemEndian) { |f| f }
          case frame
          when Frame::Method::Exchange::Declare
            exchanges << frame
          when Frame::Method::Exchange::Delete
            exchanges.reject! { |e| e.exchange_name == frame.exchange_name }
          end
        rescue IO::EOFError
          break
        end
      end
      exchanges
    end

    private def queue_bindings(vhost_dir)
      bindings = Array(Frame::Method::Queue::Bind).new
      File.open(File.join(vhost_dir, "definitions.amqp")) do |defs|
        _schema = defs.read_bytes Int32
        loop do
          frame = Frame.from_io(defs, IO::ByteFormat::SystemEndian) { |f| f }
          case frame
          when Frame::Method::Queue::Bind
            bindings << frame
          when Frame::Method::Queue::Unbind
            bindings.reject! { |b| b.exchange_name == frame.exchange_name && b.queue_name == frame.queue_name && b.routing_key == frame.routing_key && b.arguments == frame.arguments }
          end
        rescue IO::EOFError
          break
        end
      end
      bindings
    end

    private def exchange_bindings(vhost_dir)
      bindings = Array(Frame::Method::Exchange::Bind).new
      File.open(File.join(vhost_dir, "definitions.amqp")) do |defs|
        _schema = defs.read_bytes Int32
        loop do
          frame = Frame.from_io(defs, IO::ByteFormat::SystemEndian) { |f| f }
          case frame
          when Frame::Method::Exchange::Bind
            bindings << frame
          when Frame::Method::Exchange::Unbind
            bindings.reject! { |b| b.source == frame.source && b.destination == frame.destination && b.routing_key == frame.routing_key && b.arguments == frame.arguments }
          end
        rescue IO::EOFError
          break
        end
      end
      bindings
    end
  end
end
