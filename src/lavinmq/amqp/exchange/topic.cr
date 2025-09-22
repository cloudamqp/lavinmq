require "./exchange"

module LavinMQ
  module AMQP
    class TopicBindingKey
      abstract class Segment
        abstract def match?(rk : Array(String)) : Bool
      end

      class HashSegment < Segment
        def initialize(@next : Segment?)
        end

        def match?(rk : Array(String)) : Bool
          if n = @next
            i = 0
            while !rk.empty?
              rest = rk[i..]?
              break unless rest
              return true if n.match?(rest)
              i += 1
            end
            false
          else
            true
          end
        end
      end

      class StarSegment < Segment
        def initialize(@next : Segment?)
        end

        def match?(rk : Array(String)) : Bool
          v = rk.first?
          return false unless v
          rest = rk[1..]
          if check = @next
            check.match? rest
          else
            rest.empty?
          end
        end
      end

      class StringSegment < Segment
        def initialize(@s : String, @next : Segment?)
        end

        def match?(rk : Array(String)) : Bool
          v = rk.first?
          return false unless v
          rest = rk[1..]
          if check = @next
            v == @s && check.match?(rest)
          else
            v == @s && rest.empty?
          end
        end
      end

      @checker : Segment?

      def initialize(key : Array(String))
        @checker = key.reverse.reduce(nil) do |prev, v|
          case v
          when "#" then HashSegment.new(prev)
          when "*" then StarSegment.new(prev)
          else          StringSegment.new(v, prev)
          end
        end
      end

      def matches?(rk : Array(String)) : Bool
        if checker = @checker
          checker.match?(rk)
        else
          false
        end
      end
    end

    class TopicExchange < Exchange
      @bindings = Hash(Array(String), Set({AMQP::Destination, BindingKey})).new do |h, k|
        h[k] = Set({AMQP::Destination, BindingKey}).new
      end

      def type : String
        "topic"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |_rk, ds|
          ds.each.map do |d, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : AMQP::Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings[routing_key.split(".")].add?({destination, binding_key})
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil)
        rks = routing_key.split(".")
        bds = @bindings[routing_key.split(".")]
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless bds.delete({destination, binding_key})
        @bindings.delete(rks) if bds.empty?

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      # ameba:disable Metrics/CyclomaticComplexity
      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        bindings = @bindings

        return if bindings.empty?

        # optimize the case where the only binding key is '#'
        if bindings.size == 1
          bk, destinations = bindings.first
          if bk.size == 1 && bk.first == "#"
            destinations.each do |destination, _binding_key|
              yield destination
            end
          end
        end

        rk_parts = routing_key.split(".")
        bindings.each do |bks, dests|
          b = TopicBindingKey.new(bks)
          if b.matches? rk_parts
            dests.each do |destination, _binding_key|
              yield destination
            end
          end
        end
      end
    end
  end
end
