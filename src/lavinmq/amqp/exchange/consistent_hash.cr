require "../destination"
require "./exchange"
require "../../consistent_hasher.cr"

module LavinMQ
  module AMQP
    class ConsistentHashExchange < Exchange
      @hasher = ConsistentHasher(AMQP::Destination).new
      @bindings = Hash(Destination, String).new

      def type : String
        "x-consistent-hash"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.map do |d, w|
          binding_key = BindingKey.new(w, @arguments)
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end

      def bind(destination : Destination, routing_key : String, headers : AMQP::Table?)
        return false if @bindings.has_key? destination
        @bindings[destination] = routing_key
        w = weight(routing_key)
        @hasher.add(destination.name, w, destination)
        binding_key = BindingKey.new(routing_key, @arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key : String, headers : AMQP::Table?)
        return false unless @bindings.delete destination
        w = weight(routing_key)
        @hasher.remove(destination.name, w)

        binding_key = BindingKey.new(routing_key, @arguments)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      def each_destination(routing_key : String, headers : AMQP::Table?, & : Destination ->)
        key = hash_key(routing_key, headers)
        if d = @hasher.get(key)
          yield d
        end
      end

      private def weight(routing_key : String) : UInt32
        routing_key.to_u32? || raise LavinMQ::Error::PreconditionFailed.new("Routing key must to be a number")
      end

      private def hash_key(routing_key : String, headers : AMQP::Table?)
        hash_on = @arguments["x-hash-on"]?
        return routing_key unless hash_on.is_a?(String)
        return "" if headers.nil?
        case value = headers[hash_on.as(String)]?
        when String then value.as(String)
        when Nil    then ""
        else             raise LavinMQ::Error::PreconditionFailed.new("Routing header must be string")
        end
      end
    end
  end
end
