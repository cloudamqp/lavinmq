require "./exchange"
require "./consistent_hash_algorithm"
require "../destination"
require "../../hasher.cr"
require "../../consistent_hasher.cr"
require "../../jump_consistent_hasher.cr"

module LavinMQ
  module AMQP
    class ConsistentHashExchange < Exchange
      # The bindings and the hasher built from them, replaced together and
      # never mutated, so publishers on other threads route without locks
      private class State
        getter bindings : BindingSet
        getter hasher : Hasher(AMQP::Destination)

        def initialize(@bindings, @hasher)
        end
      end

      @state : Atomic(State)

      def initialize(*args, **kwargs)
        @state = Atomic(State).new(State.new(BindingSet.empty, select_hasher(Config.instance.default_consistent_hash_algorithm)))
        super(*args, **kwargs)
      end

      def type : String
        "x-consistent-hash"
      end

      def handle_arguments
        super
        if v = @arguments["x-algorithm"]?
          if hasher = v.as?(String)
            if algo = ConsistentHashAlgorithm.parse?(hasher)
              state = @state.get(:acquire)
              @state.set(State.new(state.bindings, select_hasher(algo)), :release)
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
        bindings = @state.get(:acquire).bindings
        Array(BindingDetails).new(bindings.size).tap do |details|
          bindings.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
      end

      def binding_count : Int32
        @state.get(:acquire).bindings.size
      end

      def bind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        validate_delayed_binding!(destination)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        state = @state.get(:acquire)
        bindings = state.bindings.add(destination, binding_key) || return false
        hasher = state.hasher.copy
        hasher.add(destination.name, w, destination)
        @state.set(State.new(bindings, hasher), :release)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key : String, arguments : AMQP::Table?)
        w = weight(routing_key)
        binding_key = BindingKey.new(routing_key, arguments)
        state = @state.get(:acquire)
        bindings = state.bindings.delete(destination, binding_key)
        return false if bindings.same?(state.bindings)
        # Only remove from hasher if no other bindings exist for this destination with same weight
        has_other_binding = false
        bindings.each do |e|
          has_other_binding = true if e.destination == destination && e.binding_key.routing_key == routing_key
        end
        hasher = state.hasher
        unless has_other_binding
          hasher = hasher.copy
          hasher.remove(destination.name, w)
        end
        @state.set(State.new(bindings, hasher), :release)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && bindings.empty?
        true
      end

      def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        key = hash_key(routing_key, headers)
        if d = @state.get(:acquire).hasher.get(key)
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
