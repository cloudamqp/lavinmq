require "./session"
require "./string_token_iterator"

module LavinMQ
  module MQTT
    class SubscriptionTree(T)
      @wildcard_rest = Hash(T, UInt8).new
      @wildcard_rest_filter : String?
      @plus : SubscriptionTree(T)?
      @leafs = Hash(T, UInt8).new
      @leaf_filter : String?
      # Non wildcards may be an unnecessary "optimization". We store all subscriptions without
      # wildcard in the first level. No need to make a tree out of them.
      @non_wildcards = Hash(String, Hash(T, UInt8)).new do |h, k|
        h[k] = Hash(T, UInt8).new.compare_by_identity
      end
      @sublevels = Hash(String, SubscriptionTree(T)).new

      def initialize
        @wildcard_rest.compare_by_identity
        @leafs.compare_by_identity
      end

      def subscribe(filter : String, session : T, qos : UInt8)
        if filter.index('#').nil? && filter.index('+').nil?
          @non_wildcards[filter][session] = qos
          return
        end
        subscribe(StringTokenIterator.new(filter), session, qos)
      end

      protected def subscribe(iter : StringTokenIterator, session : T, qos : UInt8)
        unless current = iter.next
          @leaf_filter = iter.to_s
          @leafs[session] = qos
          return
        end
        if current == "#"
          @wildcard_rest_filter = iter.to_s
          @wildcard_rest[session] = qos
          return
        end
        if current == "+"
          plus = (@plus ||= SubscriptionTree(T).new)
          plus.subscribe iter, session, qos
          return
        end
        if !(sublevels = @sublevels[current]?)
          sublevels = @sublevels[current] ||= SubscriptionTree(T).new
        end
        sublevels.subscribe iter, session, qos
        return
      end

      def unsubscribe(filter : String, session : T)
        if subs = @non_wildcards[filter]?
          return unless subs.delete(session).nil?
        end
        unsubscribe(StringTokenIterator.new(filter), session)
      end

      protected def unsubscribe(filter : StringTokenIterator, session : T)
        unless current = filter.next
          @leafs.delete session
          return
        end
        if current == "#"
          @wildcard_rest.delete session
        end
        if (plus = @plus) && current == "+"
          plus.unsubscribe filter, session
        end
        if sublevel = @sublevels[current]?
          sublevel.unsubscribe filter, session
          if sublevel.empty?
            @sublevels.delete current
          end
        end
      end

      # Returns wether any subscription matches the given filter
      def any?(filter : String) : Bool
        if subs = @non_wildcards[filter]?
          return !subs.empty?
        end
        any?(StringTokenIterator.new(filter))
      end

      protected def any?(filter : StringTokenIterator)
        return !@leafs.empty? unless current = filter.next
        return true if !@wildcard_rest.empty?
        return true if @plus.try &.any?(filter)
        return true if @sublevels[current]?.try &.any?(filter)
        false
      end

      def empty?
        return false unless @non_wildcards.empty? || @non_wildcards.values.all? &.empty?
        return false unless @leafs.empty?
        return false unless @wildcard_rest.empty?
        if plus = @plus
          return false unless plus.empty?
        end
        return false unless @sublevels.empty?
        true
      end

      def each_entry(topic : String, &block : (T, UInt8, String) -> _)
        if subs = @non_wildcards[topic]?
          subs.each { |s, q| yield s, q, topic }
        end
        each_entry(StringTokenIterator.new(topic), &block)
      end

      protected def each_entry(topic : StringTokenIterator, &block : (T, UInt8, String) -> _)
        unless current = topic.next
          if f = @leaf_filter
            @leafs.each { |s, q| yield s, q, f }
          end
          return
        end
        if f = @wildcard_rest_filter
          @wildcard_rest.each { |s, q| yield s, q, f }
        end
        @plus.try &.each_entry topic, &block
        if sublevel = @sublevels.fetch(current, nil)
          sublevel.each_entry topic, &block
        end
      end

      def each_entry(&block : (T, UInt8, String) -> _)
        @non_wildcards.each do |filter, entries|
          entries.each { |s, q| yield s, q, filter }
        end
        if f = @leaf_filter
          @leafs.each { |s, q| yield s, q, f }
        end
        if f = @wildcard_rest_filter
          @wildcard_rest.each { |s, q| yield s, q, f }
        end
        @plus.try &.each_entry &block
        @sublevels.each_value do |sublevel|
          sublevel.each_entry &block
        end
      end

      def inspect
        "#{self.class.name}(@wildcard_rest=#{@wildcard_rest.inspect}, @non_wildcards=#{@non_wildcards.inspect}, @plus=#{@plus.inspect}, @sublevels=#{@sublevels.inspect}, @leafs=#{@leafs.inspect})"
      end
    end
  end
end
