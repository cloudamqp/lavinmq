require "./version"
require "../stdlib/slice"
require "json"
require "amq-protocol"

class LavinMQCtl
  class DefinitionsGenerator
    def initialize(@data_dir : String)
      {"vhosts.json", "users.json"}.each do |f|
        abort "#{f} not found. Is #{@data_dir} a data directory?" unless File.exists?(File.join(@data_dir, f))
      end
      @definitions_cache = Hash(String, DefinitionState).new
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

    private class DefinitionState
      getter queues = Array(Frame::Method::Queue::Declare).new
      getter exchanges = Array(Frame::Method::Exchange::Declare).new
      getter queue_bindings = Array(Frame::Method::Queue::Bind).new
      getter exchange_bindings = Array(Frame::Method::Exchange::Bind).new
    end

    private def queues(vhost_dir)
      definitions(vhost_dir).queues
    end

    private def exchanges(vhost_dir)
      definitions(vhost_dir).exchanges
    end

    private def queue_bindings(vhost_dir)
      definitions(vhost_dir).queue_bindings
    end

    private def exchange_bindings(vhost_dir)
      definitions(vhost_dir).exchange_bindings
    end

    private def definitions(vhost_dir)
      @definitions_cache[vhost_dir] ||= load_definitions(vhost_dir)
    end

    private def load_definitions(vhost_dir)
      state = DefinitionState.new
      if File.exists?(File.join(vhost_dir, "exchanges.json")) ||
         File.exists?(File.join(vhost_dir, "queues.json")) ||
         File.exists?(File.join(vhost_dir, "bindings.json"))
        load_snapshot_definitions(vhost_dir, state)
      elsif File.exists?(legacy_path = File.join(vhost_dir, "definitions.amqp"))
        load_legacy_definitions(legacy_path, state)
      end
      state
    end

    private def load_snapshot_definitions(vhost_dir, state)
      load_json_array(File.join(vhost_dir, "exchanges.json")) do |entry|
        apply_definition_frame(state, exchange_declare_from_json(entry))
      end
      load_json_array(File.join(vhost_dir, "queues.json")) do |entry|
        apply_definition_frame(state, queue_declare_from_json(entry))
      end
      load_json_array(File.join(vhost_dir, "bindings.json")) do |entry|
        apply_definition_frame(state, binding_from_json(entry))
      end
      wal_path = File.join(vhost_dir, "definitions.wal")
      return unless File.exists?(wal_path)
      File.each_line(wal_path) do |line|
        line = line.strip
        next if line.empty?
        apply_definition_frame(state, frame_from_wal_record(JSON.parse(line)))
      end
    end

    private def load_json_array(path, &)
      return unless File.exists?(path)
      File.open(path) { |file| JSON.parse(file).as_a.each { |entry| yield entry } }
    end

    private def load_legacy_definitions(path, state)
      File.open(path) do |defs|
        _schema = defs.read_bytes Int32
        stream = AMQ::Protocol::Stream.new(defs, format: IO::ByteFormat::SystemEndian)
        loop do
          apply_definition_frame(state, stream.next_frame)
        rescue IO::EOFError
          break
        end
      end
    end

    private def apply_definition_frame(state, frame)
      case frame
      when Frame::Method::Queue::Declare
        state.queues.reject! { |q| q.queue_name == frame.queue_name }
        state.queues << frame
      when Frame::Method::Queue::Delete
        state.queues.reject! { |q| q.queue_name == frame.queue_name }
        state.queue_bindings.reject! { |b| b.queue_name == frame.queue_name }
      when Frame::Method::Queue::Bind
        state.queue_bindings << frame unless state.queue_bindings.any? { |b| queue_binding_matches?(b, frame) }
      when Frame::Method::Queue::Unbind
        state.queue_bindings.reject! { |b| queue_binding_matches?(b, frame) }
      when Frame::Method::Exchange::Declare
        state.exchanges.reject! { |e| e.exchange_name == frame.exchange_name }
        state.exchanges << frame
      when Frame::Method::Exchange::Delete
        state.exchanges.reject! { |e| e.exchange_name == frame.exchange_name }
        state.exchange_bindings.reject! { |b| b.source == frame.exchange_name || b.destination == frame.exchange_name }
        state.queue_bindings.reject! { |b| b.exchange_name == frame.exchange_name }
      when Frame::Method::Exchange::Bind
        state.exchange_bindings << frame unless state.exchange_bindings.any? { |b| exchange_binding_matches?(b, frame) }
      when Frame::Method::Exchange::Unbind
        state.exchange_bindings.reject! { |b| exchange_binding_matches?(b, frame) }
      end
    end

    private def exchange_declare_from_json(entry : JSON::Any) : Frame::Method::Exchange::Declare
      Frame::Method::Exchange::Declare.new(0_u16, 0_u16, entry["name"].as_s, entry["type"].as_s,
        false, json_bool(entry, "durable", default: true), json_bool(entry, "auto_delete"), json_bool(entry, "internal"),
        false, arguments_from_json(entry))
    end

    private def queue_declare_from_json(entry : JSON::Any) : Frame::Method::Queue::Declare
      Frame::Method::Queue::Declare.new(0_u16, 0_u16, entry["name"].as_s, false,
        json_bool(entry, "durable", default: true), json_bool(entry, "exclusive"),
        json_bool(entry, "auto_delete"), false, arguments_from_json(entry))
    end

    private def binding_from_json(entry : JSON::Any) : Frame::Method
      args = arguments_from_json(entry)
      case entry["destination_type"].as_s
      when "queue"
        Frame::Method::Queue::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, args)
      when "exchange"
        Frame::Method::Exchange::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, args)
      else
        raise "Unknown binding destination type #{entry["destination_type"]}"
      end
    end

    private def frame_from_wal_record(entry : JSON::Any) : Frame::Method
      case op = entry["op"].as_s
      when "exchange.declare" then exchange_declare_from_json(entry)
      when "exchange.delete"  then Frame::Method::Exchange::Delete.new(0_u16, 0_u16, entry["name"].as_s, false, false)
      when "exchange.bind"
        Frame::Method::Exchange::Bind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "exchange.unbind"
        Frame::Method::Exchange::Unbind.new(0_u16, 0_u16, entry["destination"].as_s, entry["source"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "queue.declare" then queue_declare_from_json(entry)
      when "queue.delete"  then Frame::Method::Queue::Delete.new(0_u16, 0_u16, entry["name"].as_s, false, false, false)
      when "queue.bind"
        Frame::Method::Queue::Bind.new(0_u16, 0_u16, entry["queue"].as_s, entry["exchange"].as_s,
          entry["routing_key"].as_s, false, arguments_from_json(entry))
      when "queue.unbind"
        Frame::Method::Queue::Unbind.new(0_u16, 0_u16, entry["queue"].as_s, entry["exchange"].as_s,
          entry["routing_key"].as_s, arguments_from_json(entry))
      else
        raise "Unknown definition WAL operation #{op}"
      end
    end

    private def arguments_from_json(entry : JSON::Any) : AMQ::Protocol::Table
      if args = entry["arguments"]?
        if hash = args.as_h?
          return AMQ::Protocol::Table.new(hash)
        end
      end
      AMQ::Protocol::Table.new
    end

    private def json_bool(entry : JSON::Any, field : String, default = false) : Bool
      if value = entry[field]?
        value.as_bool
      else
        default
      end
    end

    private def queue_binding_matches?(a : Frame::Method::Queue::Bind, b : Frame::Method::Queue::Bind | Frame::Method::Queue::Unbind) : Bool
      a.queue_name == b.queue_name &&
        a.exchange_name == b.exchange_name &&
        a.routing_key == b.routing_key &&
        a.arguments == b.arguments
    end

    private def exchange_binding_matches?(a : Frame::Method::Exchange::Bind, b : Frame::Method::Exchange::Bind | Frame::Method::Exchange::Unbind) : Bool
      a.destination == b.destination &&
        a.source == b.source &&
        a.routing_key == b.routing_key &&
        a.arguments == b.arguments
    end
  end
end
