require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      # Match spec parsed from the binding arguments at bind time, so that
      # routing doesn't have to re-parse the arguments table (which allocates
      # a String per key and a Field per value) on every published message.
      private class Binding
        private record Pair, key : String, value : AMQP::Field

        getter destinations = Set({Destination, BindingKey}).new
        @match_any : Bool
        @args_empty : Bool
        @pairs : Array(Pair)

        def initialize(args : AMQP::Table, default_match_any : Bool)
          @args_empty = args.empty?
          @match_any = case args["x-match"]?
                       when "any" then true
                       when "all" then false
                       else            default_match_any
                       end
          @pairs = Array(Pair).new
          args.each do |k, v|
            @pairs << Pair.new(k, v) unless k.starts_with?("x-")
          end
        end

        def matches?(headers : AMQP::Table?) : Bool
          if headers.nil? || headers.empty?
            @args_empty
          elsif @match_any
            @pairs.any? { |p| headers.has_entry?(p.key, p.value) }
          else
            @pairs.all? { |p| headers.has_entry?(p.key, p.value) }
          end
        end
      end

      @bindings : Sync::Shared(Hash(AMQP::Table, Binding)) = Sync::Shared.new(Hash(AMQP::Table, Binding).new, :checked)
      @default_match_any : Bool

      def initialize(@vhost : VHost, @name : String, @durable = false,
                     @auto_delete = false, @internal = false,
                     @arguments = AMQP::Table.new)
        validate!(@arguments)
        super
        @default_match_any = @arguments["x-match"]? == "any"
      end

      def type : String
        "headers"
      end

      def bindings_lock_holder : Fiber?
        @bindings.locked_by_fiber
      end

      def bindings_details : Array(BindingDetails)
        @bindings.shared do |b|
          count = b.each_value.sum(&.destinations.size)
          bds = Array(BindingDetails).new(count)
          b.each_value do |binding|
            binding.destinations.each do |d, binding_key|
              bds << BindingDetails.new(name, vhost.name, binding_key, d)
            end
          end
          bds
        end
      end

      def binding_count : Int32
        @bindings.shared do |bindings|
          bindings.each_value.sum(&.destinations.size)
        end
      end

      def bind(destination : Destination, routing_key, arguments)
        validate_delayed_binding!(destination)
        validate!(arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.lock do |b|
          binding = b[arguments] ||= Binding.new(arguments, @default_match_any)
          binding.destinations.add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        removed = false
        all_empty = false
        @bindings.lock do |b|
          binding = b[arguments]?
          next unless binding
          next unless binding.destinations.delete({destination, binding_key})
          removed = true
          b.delete(arguments) if binding.destinations.empty?
          all_empty = b.each_value.all?(&.destinations.empty?)
        end
        return false unless removed

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && all_empty
        true
      end

      private def validate!(arguments) : Nil
        if h = arguments
          if match = h["x-match"]?
            if match != "all" && match != "any"
              raise LavinMQ::Error::PreconditionFailed.new("x-match must be 'any' or 'all'")
            end
          end
        end
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |b|
          b.each_value do |binding|
            next unless binding.matches?(headers)
            binding.destinations.each do |destination, _binding_key|
              yield destination
            end
          end
        end
      end
    end
  end
end
