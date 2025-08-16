require "../destination"
require "./exchange"
require "../../consistent_hasher.cr"
require "sync/shared"

module LavinMQ
  module AMQP
    class ConsistentHashExchange < Exchange
      @hasher = Sync::Shared(ConsistentHasher(AMQP::Destination)).new(ConsistentHasher(AMQP::Destination).new)
      @bindings = Sync::Shared(Set({Destination, BindingKey})).new(Set({Destination, BindingKey}).new)

      def type : String
        "x-consistent-hash"
      end

      def bindings_details : Enumerable(BindingDetails)
        @bindings.read &.map do |destination, binding_key|
          BindingDetails.new(name, vhost.name, binding_key, destination)
        end
      end

      def bind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.write &.add?({destination, binding_key})
        @hasher.write &.add(destination.name, w, destination)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.write &.delete({destination, binding_key})
        @hasher.write &.remove(destination.name, w)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.read &.empty?
        true
      end

      def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        key = hash_key(routing_key, headers)
        if d = @hasher.read &.get(key)
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
