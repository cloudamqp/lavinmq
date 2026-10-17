require "./session"
require "./bytes_token_iterator"
require "../persistent_map"

module LavinMQ
  module MQTT
    # Subscriptions (subscriber, QoS) by topic filter, matched against
    # published topics.
    #
    # The tree is immutable. A subscribe or unsubscribe builds a new tree that
    # shares every node it doesn't change, copying only the path to the
    # filter, and publishes it with one atomic store. So publishes walk the
    # tree they read without locks, on any thread, while subscribes and
    # unsubscribes on other threads build the next one. Writers are
    # serialized by a mutex.
    class SubscriptionTree(T)
      # MQTT wildcard markers, compared by content against the Bytes tokens.
      HASH = "#".to_slice
      PLUS = "+".to_slice

      @root : Atomic(SubscriptionTreeRoot(T))
      @write_lock = Mutex.new

      def initialize
        @root = Atomic(SubscriptionTreeRoot(T)).new(SubscriptionTreeRoot(T).new)
      end

      def subscribe(filter : String, session : T, qos : UInt8)
        update &.subscribe(filter, session, qos)
      end

      def unsubscribe(filter : String, session : T)
        update &.unsubscribe(filter, session)
      end

      private def update(& : SubscriptionTreeRoot(T) -> SubscriptionTreeRoot(T)) : Nil
        @write_lock.synchronize do
          root = @root.get(:acquire)
          updated = yield root
          @root.set(updated, :release) unless updated.same?(root)
        end
      end

      # Returns wether any subscription matches the given filter
      def any?(filter : String) : Bool
        @root.get(:acquire).any?(filter)
      end

      def empty? : Bool
        @root.get(:acquire).empty?
      end

      # Total number of subscriptions (session/filter pairs) held in the tree.
      def size : Int32
        @root.get(:acquire).size
      end

      def each_entry(topic : String, &block : (T, UInt8, String) -> _)
        @root.get(:acquire).each_entry(topic, &block)
      end

      def each_entry(&block : (T, UInt8, String) -> _)
        @root.get(:acquire).each_entry(&block)
      end

      def inspect(io : IO) : Nil
        io << self.class.name << "(size=" << size << ')'
      end
    end

    # A QoS level as a reference, the value type of the subscription maps
    # (PersistentMap stores references only)
    class SubscriptionQos
      getter value : UInt8

      def initialize(@value : UInt8)
      end

      LEVELS = StaticArray[new(0u8), new(1u8), new(2u8)]

      def self.[](value : UInt8) : SubscriptionQos
        value < LEVELS.size ? LEVELS[value] : new(value)
      end
    end

    # The subscribers of one filter, with their QoS. Immutable: put and delete
    # return a new set. Up to ARRAY_MAX subscribers it's a flat array, the
    # fastest to iterate on every publish; above that a PersistentMap, so a
    # subscribe to a filter with many subscribers doesn't copy them all. It
    # goes back to an array at half the limit, so churn around the limit
    # doesn't convert every time.
    class SubscriberSet(T)
      ARRAY_MAX = 32

      @entries : Slice({T, UInt8})?
      @map : PersistentMap(T, SubscriptionQos)?
      getter size : Int32

      def self.empty : SubscriberSet(T)
        new(Slice({T, UInt8}).empty, nil)
      end

      protected def initialize(@entries : Slice({T, UInt8})?, @map : PersistentMap(T, SubscriptionQos)?)
        @size = @entries.try(&.size) || @map.try(&.size) || 0
      end

      def empty? : Bool
        @size == 0
      end

      def each(& : T, UInt8 ->) : Nil
        if entries = @entries
          entries.each { |(s, q)| yield s, q }
        elsif map = @map
          map.each { |(s, q)| yield s, q.value }
        end
      end

      # Returns self when the subscriber already has this QoS
      def put(subscriber : T, qos : UInt8) : SubscriberSet(T)
        if entries = @entries
          if i = entries.index { |(s, _)| s == subscriber }
            return self if entries[i][1] == qos
            return SubscriberSet(T).new(Slice({T, UInt8}).new(entries.size) { |j| j == i ? {subscriber, qos} : entries[j] }, nil)
          end
          if entries.size < ARRAY_MAX
            return SubscriberSet(T).new(Slice({T, UInt8}).new(entries.size + 1) { |j| j < entries.size ? entries[j] : {subscriber, qos} }, nil)
          end
          map = PersistentMap(T, SubscriptionQos).new
          entries.each { |(s, q)| map = map.put(s, SubscriptionQos[q]) }
          return SubscriberSet(T).new(nil, map.put(subscriber, SubscriptionQos[qos]))
        end
        # Not an array, so a map
        map = @map.as(PersistentMap(T, SubscriptionQos))
        updated = map.put(subscriber, SubscriptionQos[qos])
        updated.same?(map) ? self : SubscriberSet(T).new(nil, updated)
      end

      # Returns self when the subscriber isn't in the set
      def delete(subscriber : T) : SubscriberSet(T)
        if entries = @entries
          i = entries.index { |(s, _)| s == subscriber } || return self
          return SubscriberSet(T).new(Slice({T, UInt8}).new(entries.size - 1) { |j| j < i ? entries[j] : entries[j + 1] }, nil)
        end
        # Not an array, so a map
        map = @map.as(PersistentMap(T, SubscriptionQos))
        updated = map.delete(subscriber)
        return self if updated.same?(map)
        if updated.size <= ARRAY_MAX // 2
          entries = Array({T, UInt8}).new(updated.size)
          updated.each { |(s, q)| entries << {s, q.value} }
          return SubscriberSet(T).new(Slice.new(entries.to_unsafe, entries.size), nil)
        end
        SubscriberSet(T).new(nil, updated)
      end
    end

    # A topic level owning a copy of its bytes, the key of a node's sublevels.
    # It hashes and compares by content like `Bytes`, so a `Bytes` view into a
    # published topic finds it without allocating (see
    # `SubscriptionTreeNode#sublevel`).
    class SubscriptionTreeLevel
      getter bytes : Bytes

      def initialize(@bytes : Bytes)
      end

      def hash(hasher)
        @bytes.hash(hasher)
      end

      def ==(other : SubscriptionTreeLevel)
        @bytes == other.bytes
      end
    end

    # The tree: subscriptions without wildcards by their filter, and the
    # wildcard ones in a tree of nodes, one level per topic level.
    # Immutable, see SubscriptionTree.
    class SubscriptionTreeRoot(T)
      # Filters without wildcards need no tree: they are matched by the whole
      # topic.
      getter non_wildcards : PersistentMap(String, SubscriberSet(T))
      getter node : SubscriptionTreeNode(T)?

      def initialize(@non_wildcards = PersistentMap(String, SubscriberSet(T)).new,
                     @node : SubscriptionTreeNode(T)? = nil)
      end

      def subscribe(filter : String, session : T, qos : UInt8) : SubscriptionTreeRoot(T)
        if filter.index('#').nil? && filter.index('+').nil?
          subs = @non_wildcards[filter]? || SubscriberSet(T).empty
          non_wildcards = @non_wildcards.put(filter, subs.put(session, qos))
          return SubscriptionTreeRoot(T).new(non_wildcards, @node)
        end
        node = SubscriptionTreeNode(T).subscribe(@node, BytesTokenIterator.new(filter.to_slice), session, qos)
        SubscriptionTreeRoot(T).new(@non_wildcards, node)
      end

      def unsubscribe(filter : String, session : T) : SubscriptionTreeRoot(T)
        if subs = @non_wildcards[filter]?
          remaining = subs.delete(session)
          unless remaining.same?(subs)
            # Drop the entry when the last subscriber is gone, otherwise the
            # map grows with every unique filter ever subscribed to
            non_wildcards = remaining.empty? ? @non_wildcards.delete(filter) : @non_wildcards.put(filter, remaining)
            return SubscriptionTreeRoot(T).new(non_wildcards, @node)
          end
        end
        node = @node || return self
        updated = node.unsubscribe(BytesTokenIterator.new(filter.to_slice), session)
        return self if updated.same?(node)
        SubscriptionTreeRoot(T).new(@non_wildcards, updated)
      end

      def any?(filter : String) : Bool
        if subs = @non_wildcards[filter]?
          return !subs.empty?
        end
        node = @node || return false
        node.any?(BytesTokenIterator.new(filter.to_slice))
      end

      def empty? : Bool
        @non_wildcards.empty? && @node.nil?
      end

      def size : Int32
        count = 0
        @non_wildcards.each_value { |subs| count += subs.size }
        count + (@node.try(&.size) || 0)
      end

      def each_entry(topic : String, &block : (T, UInt8, String) -> _)
        if subs = @non_wildcards[topic]?
          subs.each { |s, q| yield s, q, topic }
        end
        # Nothing to walk when there are no wildcard subscriptions.
        node = @node || return
        iter = BytesTokenIterator.new(topic.to_slice)
        if topic.starts_with?('$')
          # Filters that start with a wildcard never match '$' topics (MQTT-4.7.2-1),
          # so skip the root wildcards and only follow the exact first level.
          return unless current = iter.next
          node.sublevel(current).try &.each_entry(iter, &block)
        else
          node.each_entry(iter, &block)
        end
      end

      def each_entry(&block : (T, UInt8, String) -> _)
        @non_wildcards.each do |(filter, subs)|
          subs.each { |s, q| yield s, q, filter }
        end
        @node.try &.each_entry(&block)
      end
    end

    # One level of the wildcard tree. Immutable: subscribe and unsubscribe
    # return a copy with the change that shares the unchanged children. A node
    # that would hold nothing is nil instead, so no empty nodes are kept.
    class SubscriptionTreeNode(T)
      # Subscribers whose filter ends at this level, and that filter
      getter leafs : SubscriberSet(T)?
      getter leaf_filter : String?
      # Subscribers whose filter has '#' at this level, and that filter
      getter wildcard_rest : SubscriberSet(T)?
      getter wildcard_rest_filter : String?
      # The subtree of filters with '+' at this level
      getter plus : SubscriptionTreeNode(T)?
      getter sublevels : PersistentMap(SubscriptionTreeLevel, SubscriptionTreeNode(T))?

      def initialize(@leafs : SubscriberSet(T)?, @leaf_filter : String?,
                     @wildcard_rest : SubscriberSet(T)?, @wildcard_rest_filter : String?,
                     @plus : SubscriptionTreeNode(T)?, @sublevels : PersistentMap(SubscriptionTreeLevel, SubscriptionTreeNode(T))?)
      end

      def self.subscribe(node : SubscriptionTreeNode(T)?, filter : BytesTokenIterator, session : T, qos : UInt8) : SubscriptionTreeNode(T)
        node ||= new(nil, nil, nil, nil, nil, nil)
        node.subscribe(filter, session, qos)
      end

      protected def subscribe(filter : BytesTokenIterator, session : T, qos : UInt8) : SubscriptionTreeNode(T)
        unless current = filter.next
          leafs = (@leafs || SubscriberSet(T).empty).put(session, qos)
          return copy(leafs: leafs, leaf_filter: filter.to_s)
        end
        if current == SubscriptionTree::HASH
          rest = (@wildcard_rest || SubscriberSet(T).empty).put(session, qos)
          return copy(wildcard_rest: rest, wildcard_rest_filter: filter.to_s)
        end
        if current == SubscriptionTree::PLUS
          return copy(plus: SubscriptionTreeNode(T).subscribe(@plus, filter, session, qos))
        end
        sublevel = SubscriptionTreeNode(T).subscribe(sublevel(current), filter, session, qos)
        # dup the token to own it as a map key (subscribe is cold)
        sublevels = (@sublevels || PersistentMap(SubscriptionTreeLevel, SubscriptionTreeNode(T)).new).put(SubscriptionTreeLevel.new(current.dup), sublevel)
        copy(sublevels: sublevels)
      end

      # Returns self if the session wasn't subscribed to the filter, and nil
      # if nothing is left in this node
      protected def unsubscribe(filter : BytesTokenIterator, session : T) : SubscriptionTreeNode(T)?
        unless current = filter.next
          return without_leaf(session)
        end
        node = self
        node = node.without_wildcard_rest(session) if current == SubscriptionTree::HASH
        node = node.with_plus_unsubscribed(filter, session) if current == SubscriptionTree::PLUS
        node = node.with_sublevel_unsubscribed(current, filter, session)
        node.same?(self) ? self : node.presence
      end

      private def without_leaf(session : T) : SubscriptionTreeNode(T)?
        leafs = @leafs || return self
        remaining = leafs.delete(session)
        return self if remaining.same?(leafs)
        remaining.empty? ? copy(leafs: nil, leaf_filter: nil).presence : copy(leafs: remaining)
      end

      protected def without_wildcard_rest(session : T) : SubscriptionTreeNode(T)
        rest = @wildcard_rest || return self
        remaining = rest.delete(session)
        return self if remaining.same?(rest)
        remaining.empty? ? copy(wildcard_rest: nil, wildcard_rest_filter: nil) : copy(wildcard_rest: remaining)
      end

      protected def with_plus_unsubscribed(filter : BytesTokenIterator, session : T) : SubscriptionTreeNode(T)
        plus = @plus || return self
        updated = plus.unsubscribe(filter, session)
        updated.same?(plus) ? self : copy(plus: updated)
      end

      protected def with_sublevel_unsubscribed(level : Bytes, filter : BytesTokenIterator, session : T) : SubscriptionTreeNode(T)
        sublevels = @sublevels || return self
        sublevel = sublevel(level) || return self
        updated = sublevel.unsubscribe(filter, session)
        return self if updated.same?(sublevel)
        remaining = if updated
                      sublevels.put(SubscriptionTreeLevel.new(level.dup), updated)
                    else
                      sublevels.delete(SubscriptionTreeLevel.new(level))
                    end
        copy(sublevels: remaining.empty? ? nil : remaining)
      end

      # The child for a topic level, looked up by a view into the topic
      def sublevel(level : Bytes) : SubscriptionTreeNode(T)?
        @sublevels.try &.find_value(hash: level.hash) { |k| k.bytes == level }
      end

      protected def any?(filter : BytesTokenIterator) : Bool
        return !@leafs.nil? unless current = filter.next
        return true unless @wildcard_rest.nil?
        return true if @plus.try &.any?(filter)
        return true if sublevel(current).try &.any?(filter)
        false
      end

      protected def size : Int32
        count = (@leafs.try(&.size) || 0) + (@wildcard_rest.try(&.size) || 0)
        count += @plus.try(&.size) || 0
        @sublevels.try &.each_value { |sublevel| count += sublevel.size }
        count
      end

      protected def each_entry(topic : BytesTokenIterator, &block : (T, UInt8, String) -> _)
        unless current = topic.next
          if (leafs = @leafs) && (f = @leaf_filter)
            leafs.each { |s, q| yield s, q, f }
          end
          # "#" also matches its parent level, so "a/#" matches "a" (MQTT 3.1.1 §4.7.1.2)
          if (rest = @wildcard_rest) && (f = @wildcard_rest_filter)
            rest.each { |s, q| yield s, q, f }
          end
          return
        end
        if (rest = @wildcard_rest) && (f = @wildcard_rest_filter)
          rest.each { |s, q| yield s, q, f }
        end
        @plus.try &.each_entry topic, &block
        if sublevel = sublevel(current)
          sublevel.each_entry topic, &block
        end
      end

      protected def each_entry(&block : (T, UInt8, String) -> _)
        if (leafs = @leafs) && (f = @leaf_filter)
          leafs.each { |s, q| yield s, q, f }
        end
        if (rest = @wildcard_rest) && (f = @wildcard_rest_filter)
          rest.each { |s, q| yield s, q, f }
        end
        @plus.try &.each_entry &block
        @sublevels.try &.each_value do |sublevel|
          sublevel.each_entry &block
        end
      end

      protected def copy(leafs = @leafs, leaf_filter = @leaf_filter,
                         wildcard_rest = @wildcard_rest, wildcard_rest_filter = @wildcard_rest_filter,
                         plus = @plus, sublevels = @sublevels) : SubscriptionTreeNode(T)
        SubscriptionTreeNode(T).new(leafs, leaf_filter, wildcard_rest, wildcard_rest_filter, plus, sublevels)
      end

      # nil for a node that holds nothing
      protected def presence : SubscriptionTreeNode(T)?
        self unless @leafs.nil? && @wildcard_rest.nil? && @plus.nil? && @sublevels.nil?
      end
    end
  end
end
