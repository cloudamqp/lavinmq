require "json"
require "./topic"
require "./policy"
require "./consistent_hash"

module LavinMQWasm
  def self.route_direct(bindings : Array({String, String}), routing_key : String) : Array(String)
    bindings.select { |_, bk| bk == routing_key }.map(&.[0]).uniq
  end

  def self.route_fanout(bindings : Array({String, String})) : Array(String)
    bindings.map(&.[0]).uniq
  end

  # bindings: Array of {queue_name, header_arguments}
  def self.route_headers(
    bindings : Array({String, Hash(String, JSON::Any)}),
    message_headers : Hash(String, JSON::Any),
    exchange_x_match : String = "all",
  ) : Array(String)
    matched = [] of String
    bindings.each do |queue, args|
      x_match = args["x-match"]?.try(&.as_s?) || exchange_x_match
      filtered = args.reject { |k, _| k.starts_with?("x-") }
      is_match = case x_match
                 when "any"
                   filtered.any? { |k, v| message_headers[k]? == v }
                 else
                   filtered.all? { |k, v| message_headers[k]? == v }
                 end
      matched << queue if is_match
    end
    matched.uniq
  end

  def self.route(request : JSON::Any) : Array(String)
    exchange_type = request["exchange_type"].as_s
    raw_bindings = request["bindings"].as_a
    routing_key = request["routing_key"]?.try(&.as_s) || ""
    message_headers = request["headers"]?.try(&.as_h?) || Hash(String, JSON::Any).new
    exchange_args = request["exchange_arguments"]?.try(&.as_h?) || Hash(String, JSON::Any).new

    case exchange_type
    when "direct"
      bindings = raw_bindings.map { |b| {b["queue"].as_s, b["routing_key"].as_s} }
      route_direct(bindings, routing_key)
    when "fanout"
      bindings = raw_bindings.map { |b| {b["queue"].as_s, ""} }
      route_fanout(bindings)
    when "topic"
      bindings = raw_bindings.map { |b| {b["queue"].as_s, b["routing_key"].as_s} }
      route_topic(bindings, routing_key)
    when "headers"
      bindings = raw_bindings.map { |b| {b["queue"].as_s, b["arguments"]?.try(&.as_h?) || Hash(String, JSON::Any).new} }
      exchange_x_match = request["x_match"]?.try(&.as_s) || "all"
      route_headers(bindings, message_headers, exchange_x_match)
    when "x-consistent-hash"
      bindings = raw_bindings.map { |b| {b["queue"].as_s, b["routing_key"].as_s} }
      route_consistent_hash(bindings, routing_key, message_headers, exchange_args)
    else
      [] of String
    end
  end
end
