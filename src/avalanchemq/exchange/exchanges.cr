require "./direct"
require "./fanout"
require "./topic"
require "./headers"
require "./federation"
require "./consistent_hash"

module AvalancheMQ
  module Exchanges
  def self.make(vhost, name, type, durable, auto_delete, internal, arguments)
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
