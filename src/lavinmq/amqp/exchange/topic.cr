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
      # The bindings as a flat array, which publishes scan as every binding
      # key has to be matched: an array scans much faster than the trie.
      # Built from the version of @bindings it is tagged with.
      private class Routes
        getter version : UInt64
        getter entries : Array({TopicBindingKey, BindingSet})

        def initialize(@version, @entries)
        end
      end

      # Binding key => bindings. Both levels are replaced, never mutated, so
      # publishers on other threads route without locks.
      @bindings = CowMap(TopicBindingKey, BindingSet).new
      # Bumped after every change to @bindings
      @bindings_version = Atomic(UInt64).new(0u64)
      # Rebuilt by the first publish after a change, so that a bind costs
      # O(1) array work and loading n bindings stays O(n), not O(n²)
      @routes = Atomic(Routes).new(Routes.new(0u64, Array({TopicBindingKey, BindingSet}).new))
      # Serializes binds and unbinds, which read, change and replace a key's
      # bindings
      @bindings_lock = Mutex.new

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
        @bindings_lock.synchronize do
          current = @bindings[rk]? || BindingSet.empty
          bindings = current.add(destination, binding_key) || return false
          @bindings[rk] = bindings
          @bindings_version.add(1u64, :release)
        end
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_bound(data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, arguments = nil)
        rk = TopicBindingKey.new(routing_key.split("."))
        binding_key = BindingKey.new(routing_key, arguments)
        @bindings_lock.synchronize do
          current = @bindings[rk]? || return false
          bindings = current.delete(destination, binding_key)
          return false if bindings.same?(current)
          if bindings.empty?
            @bindings.delete(rk)
          else
            @bindings[rk] = bindings
          end
          @bindings_version.add(1u64, :release)
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_unbound(data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      # The routes for the current bindings. The version is read before the
      # bindings, so routes built here hold at least every change up to it; a
      # bind returns only after bumping the version, so a publish after it
      # rebuilds and sees the binding. Concurrent rebuilds are harmless: an
      # older one that's published last is rebuilt again by the next publish.
      private def current_routes : Routes
        version = @bindings_version.get(:acquire)
        routes = @routes.get(:acquire)
        return routes if routes.version == version
        snapshot = @bindings.snapshot
        entries = Array({TopicBindingKey, BindingSet}).new(snapshot.size)
        snapshot.each { |kv| entries << kv }
        # In memory order of the keys, the first thing the scan reads per
        # entry: scanning in the map's hash order jumps around the heap and
        # measured over 10% slower (the GC doesn't move objects)
        entries.sort_by! { |(key, _)| key.object_id }
        routes = Routes.new(version, entries)
        @routes.set(routes, :release)
        routes
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        routes = current_routes.entries
        return if routes.empty?

        # optimize the case where the only binding key is '#'
        if routes.size == 1
          bk, destinations = routes.first
          if bk.acts_as_fanout?
            destinations.each_destination { |d| yield d }
            return
          end
        end

        rk = RkIterator.new(routing_key.to_slice)
        # Index and bounds in locals: measured faster than Array#each in this
        # loop, which is inlined into a large method
        ptr = routes.to_unsafe
        size = routes.size
        i = 0
        while i < size
          bks, destinations = ptr[i]
          i += 1
          if bks.matches? rk
            destinations.each_destination { |d| yield d }
          end
        end
      end
    end
  end
end
