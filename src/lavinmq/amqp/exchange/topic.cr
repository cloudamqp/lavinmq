require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    struct RkIterator
      getter value : Bytes

      def initialize(raw : Bytes)
        if first_dot = raw.index '.'.ord
          @value = raw[0, first_dot]
          @raw = raw[(first_dot + 1)..]? || Bytes.empty
        else
          @value = raw
          @raw = Bytes.empty
        end
      end

      def next : RkIterator?
        self.class.new(@raw) unless @raw.empty?
      end
    end

    class TopicBindingKey
      abstract class Segment
        abstract def match?(rk) : Bool
      end

      class HashSegment < Segment
        def initialize(@next : Segment?)
        end

        def match?(rk) : Bool
          if n = @next
            return true if n.match?(rk)
            return false unless rk

            loop do
              rk = rk.next
              break unless rk
              return true if n.match?(rk)
            end
            return false
          end
          true
        end
      end

      class StarSegment < Segment
        def initialize(@next : Segment?)
        end

        def match?(rk) : Bool
          return false unless rk
          if check = @next
            n = rk.next
            check.match?(n)
          else
            rk.next.nil?
          end
        end
      end

      class StringSegment < Segment
        def initialize(@s : Bytes, @next : Segment?)
        end

        def match?(rk) : Bool
          return false unless rk
          return false unless rk.value == @s
          if check = @next
            n = rk.next
            check.match?(n)
          else
            rk.next.nil?
          end
        end
      end

      @checker : Segment?

      def initialize(@key : Array(String))
        @checker = @key.reverse_each.reduce(nil) do |prev, v|
          case v
          when "#" then HashSegment.new(prev)
          when "*" then StarSegment.new(prev)
          else          StringSegment.new(v.to_slice, prev)
          end
        end
      end

      def matches?(rk) : Bool
        return false unless rk
        if checker = @checker
          checker.match?(rk)
        else
          false
        end
      end

      def acts_as_fanout?
        @key.size == 1 && @key.first == "#"
      end

      def_equals_and_hash @key
    end

    class TopicExchange < Exchange
      @bindings : Sync::Shared(Hash(TopicBindingKey, Set({AMQP::Destination, BindingKey}))) = Sync::Shared.new(Hash(TopicBindingKey, Set({AMQP::Destination, BindingKey})).new do |h, k|
        h[k] = Set({AMQP::Destination, BindingKey}).new
      end)

      def type : String
        "topic"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.shared do |bindings|
          bindings.each_value.flat_map do |ds|
            ds.map do |d, binding_key|
              BindingDetails.new(name, vhost.name, binding_key, d)
            end
          end.to_a
        end.each
      end

      def bind(destination : AMQP::Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        rk = TopicBindingKey.new(routing_key.split("."))
        added = @bindings.lock do |bindings|
          bindings[rk].add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil)
        rks = routing_key.split(".")
        rk = TopicBindingKey.new(rks)
        binding_key = BindingKey.new(routing_key, arguments)
        result = @bindings.lock do |bindings|
          bds = bindings[rk]
          deleted = bds.delete({destination, binding_key})
          if deleted
            bindings.delete(rk) if bds.empty?
            {true, @auto_delete && bindings.each_value.all?(&.empty?)}
          else
            {false, false}
          end
        end
        return false unless result[0]

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if result[1]
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |bindings|
          next if bindings.empty?

          # optimize the case where the only binding key is '#'
          if bindings.size == 1
            bk, destinations = bindings.first
            if bk.acts_as_fanout?
              destinations.each do |destination, _binding_key|
                yield destination
              end
              next
            end
          end

          rk = RkIterator.new(routing_key.to_slice)
          bindings.each do |bks, dests|
            if bks.matches? rk
              dests.each do |destination, _binding_key|
                yield destination
              end
            end
          end
        end
      end
    end
  end
end
