require "sync/shared"
require "./exchange"
require "./consistent_hash_algorithm"
require "../destination"
require "../../hasher.cr"
require "../../consistent_hasher.cr"
require "../../jump_consistent_hasher.cr"

module LavinMQ
  module AMQP
    class ConsistentHashExchange < Exchange
      @hasher : Hasher(AMQP::Destination)
      @bindings : Sync::Shared(Set({Destination, BindingKey})) = Sync::Shared.new(Set({Destination, BindingKey}).new, :unchecked)

      def initialize(*args, **kwargs)
        super(*args, **kwargs)
        @hasher = select_hasher(Config.instance.default_consistent_hash_algorithm)
      end

      def type : String
        "x-consistent-hash"
      end

      def handle_arguments
        super
        if v = @arguments["x-algorithm"]?
          if hasher = v.as?(String)
            if algo = ConsistentHashAlgorithm.parse?(hasher)
              @hasher = select_hasher(algo)
              @effective_args << "x-algorithm"
            end
          end
        end
        @effective_args << "x-hash-on" if @arguments["x-hash-on"]?
      end

      private def select_hasher(option : ConsistentHashAlgorithm)
        case option
        in .jump?
          JumpConsistentHasher(AMQP::Destination).new
        in .ring?
          RingConsistentHasher(AMQP::Destination).new
        end
      end

      def bindings_details : Array(BindingDetails)
        @bindings.shared do |b|
          b.map do |destination, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, destination)
          end
        end
      end

      def bind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        validate_delayed_binding!(destination)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.lock do |b|
                              next false unless b.add?({destination, binding_key})
                              @hasher.add(destination.name, w, destination)
                              true
                            end
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        removed = false
        empty = false
        @bindings.lock do |b|
          next unless b.delete({destination, binding_key})
          removed = true
          # Only remove from hasher if no other bindings exist for this destination with same weight
          has_other_binding = b.any? do |d, bk|
            d == destination && bk.routing_key == routing_key
          end
          @hasher.remove(destination.name, w) unless has_other_binding
          empty = b.empty?
        end
        return false unless removed
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && empty
        true
      end

      def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        key = hash_key(routing_key, headers)
        @bindings.shared do |_|
          if d = @hasher.get(key)
            yield d
          end
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
