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
      # Binding key => bindings. Both levels are replaced, never mutated, so
      # publishers on other threads route without locks.
      @bindings = CowMap(TopicBindingKey, BindingSet).new

      def type : String
        "topic"
      end

      def bindings_details : Array(BindingDetails)
        details = Array(BindingDetails).new
        @bindings.each do |(_, bindings)|
          bindings.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
        details
      end

      def binding_count : Int32
        count = 0
        @bindings.each_value { |bindings| count += bindings.size }
        count
      end

      def bind(destination : AMQP::Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        rk = TopicBindingKey.new(routing_key.split("."))
        current = @bindings[rk]? || BindingSet.empty
        @bindings[rk] = current.add(destination, binding_key) || return false
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil)
        rk = TopicBindingKey.new(routing_key.split("."))
        current = @bindings[rk]? || return false
        binding_key = BindingKey.new(routing_key, arguments)
        bindings = current.delete(destination, binding_key)
        return false if bindings.same?(current)
        if bindings.empty?
          @bindings.delete(rk)
        else
          @bindings[rk] = bindings
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        bindings = @bindings.snapshot
        return if bindings.empty?

        # optimize the case where the only binding key is '#'
        if bindings.size == 1
          bindings.each do |(bk, destinations)|
            if bk.acts_as_fanout?
              destinations.each_destination { |d| yield d }
              return
            end
          end
        end

        rk = RkIterator.new(routing_key.to_slice)
        bindings.each do |(bks, destinations)|
          if bks.matches? rk
            destinations.each_destination { |d| yield d }
          end
        end
      end
    end
  end
end
