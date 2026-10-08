require "../../persistent_map"
require "../binding_key"

module LavinMQ
  module AMQP
    # An immutable set of bindings (destination + binding key). `add` and
    # `delete` return a new set and leave this one untouched, so an exchange
    # can route with the set it read while a bind or unbind on another thread
    # publishes the next version.
    #
    # Most sets are small, and a flat array is the fastest to iterate when
    # routing, so a set starts as an array and copies it on every change.
    # Above ARRAY_MAX entries that copy gets expensive (a fanout exchange can
    # have thousands of queues), so the set becomes a PersistentMap, where a
    # change copies only a few small nodes.
    abstract class BindingSet
      ARRAY_MAX = 64

      class Entry
        getter destination : Queue | Exchange
        getter binding_key : BindingKey

        def initialize(@destination, @binding_key)
        end

        def_equals_and_hash @destination, @binding_key
      end

      def self.empty : BindingSet
        ArrayBindingSet.new(Slice(Entry).empty)
      end

      abstract def size : Int32
      abstract def each(& : Entry ->) : Nil

      # Returns nil if the binding is already in the set
      abstract def add(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet?

      # Returns self if the binding isn't in the set
      abstract def delete(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet

      def empty? : Bool
        size == 0
      end

      def each_destination(& : (Queue | Exchange) ->) : Nil
        each { |e| yield e.destination }
      end
    end

    class ArrayBindingSet < BindingSet
      def initialize(@entries : Slice(Entry))
      end

      def size : Int32
        @entries.size
      end

      def each(& : Entry ->) : Nil
        @entries.each { |e| yield e }
      end

      def add(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet?
        entry = Entry.new(destination, binding_key)
        return if @entries.includes?(entry)
        if @entries.size >= ARRAY_MAX
          map = PersistentMap(Entry, Entry).new
          @entries.each { |e| map = map.put(e, e) }
          return MapBindingSet.new(map.put(entry, entry))
        end
        entries = Slice(Entry).new(@entries.size + 1) { |i| i < @entries.size ? @entries[i] : entry }
        ArrayBindingSet.new(entries)
      end

      def delete(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet
        entry = Entry.new(destination, binding_key)
        idx = @entries.index(entry) || return self
        entries = Slice(Entry).new(@entries.size - 1) { |i| i < idx ? @entries[i] : @entries[i + 1] }
        ArrayBindingSet.new(entries)
      end
    end

    class MapBindingSet < BindingSet
      def initialize(@map : PersistentMap(Entry, Entry))
      end

      def size : Int32
        @map.size
      end

      def each(& : Entry ->) : Nil
        @map.each_key { |e| yield e }
      end

      def add(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet?
        entry = Entry.new(destination, binding_key)
        return if @map.has_key?(entry)
        MapBindingSet.new(@map.put(entry, entry))
      end

      def delete(destination : Queue | Exchange, binding_key : BindingKey) : BindingSet
        map = @map.delete(Entry.new(destination, binding_key))
        return self if map.same?(@map)
        # Back to an array once well below the limit, so that adding and
        # removing one binding around the limit doesn't convert every time
        if map.size <= ARRAY_MAX // 2
          keys = map.keys
          return ArrayBindingSet.new(Slice.new(keys.to_unsafe, keys.size))
        end
        MapBindingSet.new(map)
      end
    end
  end
end
