require "./exchange"

module LavinMQ
  module AMQP
    USE_NEW = true

    class TopicKey
      enum SegmentResult
        INVALID
        VALID
        NEXT
      end

      abstract struct Segment
        abstract def match?(v) : SegmentResult
      end

      struct HashSegment < Segment
        @done = false

        def initialize(@until : String?)
        end

        def match?(v) : SegmentResult
          if (c = @until) && c == v
            return SegmentResult::NEXT
          end
          SegmentResult::VALID
        end
      end

      struct StarSegment < Segment
        @done = false

        def match?(v) : SegmentResult
          return SegmentResult::NEXT if @done
          @done = true
          SegmentResult::VALID
        end
      end

      struct StringSegment < Segment
        @done = false

        def initialize(@s : String)
        end

        def match?(v) : SegmentResult
          return SegmentResult::NEXT if @done
          @done = true
          v == @s ? SegmentResult::VALID : SegmentResult::INVALID
        end
      end

      @checkers : Array(Segment)

      def initialize(key : Array(String))
        l = key.size
        i = 0

        puts ""
        puts "############################# "
        pp key
        parts = Array(Segment).new(l)
        while i < l
          v = key[i]
          seg = case v
                when "#" then HashSegment.new(key[i + 1]?)
                when "*" then StarSegment.new
                else          StringSegment.new(v)
                end
          parts.push seg
          i += 1
        end
        @checkers = parts
      end

      def matches?(routing_key : Array(String))
        pp routing_key, @checkers
        checker = @checkers.shift?
        puts "checker=#{checker}"
        return false unless checker

        i = 0
        # use checker until it returns NEXT, then get next checker and continue
        while i < routing_key.size
          seg = routing_key[i]
          res = checker.match?(seg)
          pp "i=#{i} seg=#{seg} checker=#{checker} res=#{res}"
          case res
          when SegmentResult::NEXT
            checker = @checkers.shift?
            return false unless checker
          when SegmentResult::VALID
            i += 1
          when SegmentResult::INVALID
            return false
          end
        end
        return false unless @checkers.empty?
        true
      end

      # def matches?(routing_key : Array(String))
      #   puts " "
      #   puts "segments=#{routing_key} segments_to_check=#{routing_key[0..]}"
      #   puts "checkers=#{@parts.map(&.to_s)} rest_checkers=#{@parts[0..].map(&.to_s)}"
      #   i = 0
      #   prev_checker = nil
      #   routing_key.each do |seg|
      #     next if prev_checker.try(&.is_a?(HashSegment))
      #     checker = @parts[i]?
      #     return false unless checker
      #     prev_checker = checker
      #     return false unless checker.match?(seg)
      #     i += 1
      #   end

      #   rest = @parts.size - i
      #   return false if routing_key.size == i && @parts.size > i

      #   puts " "
      #   puts "segments=#{routing_key} segments_to_check=#{routing_key[-rest..]}"
      #   puts "checkers=#{@parts.map(&.to_s)} rest_checkers=#{@parts[i..].map(&.to_s)}"
      #   @parts[i..].each_with_index do |checker, i|
      #     seg = routing_key[-rest]
      #     # puts "rest=#{rest} routing_key=#{routing_key} seg=#{seg}"
      #     return false unless checker.match?(seg)
      #     rest -= 1
      #   end
      #   true
      # end
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
          if bk.size == 1
            if bk.first == "#"
              destinations.each do |destination, _binding_key|
                yield destination
              end
            end
          end
        end

        rk_parts = routing_key.split(".")
        bindings.each do |bks, dests|
          ok = if USE_NEW
                 rk = TopicKey.new(bks)
                 rk.matches? rk_parts
               else
                 topic_bks_matches_rk?(rk_parts, bks)
               end
          if ok
            dests.each do |destination, _binding_key|
              yield destination
            end
          end
        end
      end

      private def topic_bks_matches_rk?(rk_parts, bks)
        ok = false
        prev_hash = false
        size = bks.size # binding keys can max be 256 chars long anyway
        j = 0
        i = 0
        bks.each do |part|
          if rk_parts.size <= j
            ok = false
            break
          end
          case part
          when "#"
            j += 1
            prev_hash = true
            ok = true
          when "*"
            prev_hash = false
            # Is this the last bk and the last rk?
            if size == i + 1 && rk_parts.size == j + 1
              ok = true
              break
              # More than 1 rk left ok move on
            elsif rk_parts.size > j + 1
              j += 1
              i += 1
              next
            else
              ok = false
              j += 1
            end
          else
            if prev_hash
              if size == (i + 1)
                ok = rk_parts.last == part
                j += 1
              else
                ok = false
                rk_parts[j..-1].each do |rk_part|
                  j += 1
                  ok = part == rk_part
                  break if ok
                end
              end
            else
              # Is this the last bk but not the last rk?
              if size == i + 1 && rk_parts.size > j + 1
                ok = false
              else
                ok = rk_parts[j] == part
              end
              j += 1
            end
          end
          break unless ok
          i += 1
        end
        ok
      end
    end
  end
end
