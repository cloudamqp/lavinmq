require "./session"
require "./string_token_iterator"

module LavinMQ
  module MQTT
    class SubscriptionTree(T)
      @wildcard_rest = Hash(T, UInt8).new
      @plus : SubscriptionTree(T)?
      @leafs = Hash(T, UInt8).new
      # Non wildcards may be an unnecessary "optimization". We store all subscriptions without
      # wildcard in the first level. No need to make a tree out of them.
      @non_wildcards = Hash(String, Hash(T, UInt8)).new do |h, k|
        h[k] = Hash(T, UInt8).new
        h[k].compare_by_identity
        h[k]
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

      protected def subscribe(filter : StringTokenIterator, session : T, qos : UInt8)
        unless current = filter.next
          @leafs[session] = qos
          return
        end
        if current == "#"
          @wildcard_rest[session] = qos
          return
        end
        if current == "+"
          plus = (@plus ||= SubscriptionTree(T).new)
          plus.subscribe filter, session, qos
          return
        end
        if !(sublevels = @sublevels[current]?)
          sublevels = @sublevels[current] = SubscriptionTree(T).new
        end
        sublevels.subscribe filter, session, qos
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
        if sublevels = @sublevels
          return false unless sublevels.empty?
        end
        true
      end

      def each_entry(topic : String, &block : (T, UInt8) -> _)
        if subs = @non_wildcards[topic]?
          subs.each &block
        end
        each_entry(StringTokenIterator.new(topic), &block)
      end

      protected def each_entry(topic : StringTokenIterator, &block : (T, UInt8) -> _)
        unless current = topic.next
          @leafs.each &block
          return
        end
        @wildcard_rest.each &block
        @plus.try &.each_entry topic, &block
        if sublevel = @sublevels.fetch(current, nil)
          sublevel.each_entry topic, &block
        end
      end

      def each_entry(&block : (T, UInt8) -> _)
        @non_wildcards.each do |_, entries|
          entries.each &block
        end
        @leafs.each &block
        @wildcard_rest.each &block
        @plus.try &.each_entry &block
        @sublevels.each do |_, sublevel|
          sublevel.each_entry &block
        end
      end

      def inspect
        "#{self.class.name}(@wildcard_rest=#{@wildcard_rest.inspect}, @non_wildcards=#{@non_wildcards.inspect}, @plus=#{@plus.inspect}, @sublevels=#{@sublevels.inspect}, @leafs=#{@leafs.inspect})"
      end
    end
  end
end
