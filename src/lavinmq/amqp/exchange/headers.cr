require "./exchange"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      # Match spec parsed from the binding arguments at bind time, so that
      # routing doesn't have to re-parse the arguments table (which allocates
      # a String per key and a Field per value) on every published message.
      private class Binding
        private record Pair, key : String, value : AMQP::Field

        # Replaced, never mutated (see `with`)
        getter destinations : BindingSet
        @match_any : Bool
        @args_empty : Bool
        @pairs : Array(Pair)

        def initialize(args : AMQP::Table, default_match_any : Bool)
          @destinations = BindingSet.empty
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

        protected def initialize(other : Binding, @destinations : BindingSet)
          @args_empty = other.@args_empty
          @match_any = other.@match_any
          @pairs = other.@pairs
        end

        # The same match spec with another set of destinations
        def with(destinations : BindingSet) : Binding
          Binding.new(self, destinations)
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

      # Arguments => match spec and bindings. Replaced, never mutated, so
      # publishers on other threads route without locks.
      @bindings = CowMap(AMQP::Table, Binding).new
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

      def bindings_details : Array(BindingDetails)
        details = Array(BindingDetails).new
        @bindings.each_value do |binding|
          binding.destinations.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
        details
      end

      def binding_count : Int32
        count = 0
        @bindings.each_value { |binding| count += binding.destinations.size }
        count
      end

      def bind(destination : Destination, routing_key, arguments)
        validate_delayed_binding!(destination)
        validate!(arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        binding = @bindings[arguments]? || Binding.new(arguments, @default_match_any)
        destinations = binding.destinations.add(destination, binding_key) || return false
        @bindings[arguments] = binding.with(destinations)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_bound(data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        binding = @bindings[arguments]? || return false
        destinations = binding.destinations.delete(destination, binding_key)
        return false if destinations.same?(binding.destinations)
        if destinations.empty?
          @bindings.delete(arguments)
        else
          @bindings[arguments] = binding.with(destinations)
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_unbound(data)

        delete if @auto_delete && @bindings.empty?
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

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        @bindings.each_value do |binding|
          next unless binding.matches?(headers)
          binding.destinations.each_destination { |d| yield d }
        end
      end
    end
  end
end
