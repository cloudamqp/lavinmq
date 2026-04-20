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

      def initialize(key : String)
        initialize(key.split("."))
      end

      def matches?(rk) : Bool
        return false unless rk
        if checker = @checker
          checker.match?(rk)
        else
          false
        end
      end

      def matches?(routing_key : String) : Bool
        return false if routing_key.empty? && @checker
        matches?(RkIterator.new(routing_key.to_slice))
      end

      def acts_as_fanout?
        @key.size == 1 && @key.first == "#"
      end

      def_equals_and_hash @key
    end
  end
end
