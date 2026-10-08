require "./string_token_iterator"

module LavinMQ
  module MQTT
    class TopicTree(TEntity)
      @sublevels = Hash(String, TopicTree(TEntity)).new do |h, k|
        h[k] = TopicTree(TEntity).new
      end

      @leafs = Hash(String, Tuple(String, TEntity)).new

      def initialize
      end

      def insert(topic : String, entity : TEntity) : TEntity?
        insert(StringTokenIterator.new(topic, '/'), entity)
      end

      def insert(topic : StringTokenIterator, entity : TEntity) : TEntity?
        current = topic.next
        raise ArgumentError.new "topic cannot be empty" unless current
        if topic.next?
          @sublevels[current].insert(topic, entity)
        else
          old_value = @leafs[current]?
          @leafs[current] = {topic.to_s, entity}
          old_value.try &.last
        end
      end

      def []?(topic : String) : TEntity?
        self[StringTokenIterator.new(topic, '/')]?
      end

      def []?(topic : StringTokenIterator) : TEntity?
        current = topic.next
        if topic.next?
          return unless @sublevels.has_key?(current)
          @sublevels[current][topic]?
        else
          @leafs[current]?.try &.last
        end
      end

      def delete(topic : String)
        delete(StringTokenIterator.new(topic, '/'))
      end

      def delete(topic : StringTokenIterator)
        current = topic.next
        if topic.next?
          return unless @sublevels.has_key?(current)
          deleted = @sublevels[current].delete(topic)
          if @sublevels[current].empty?
            @sublevels.delete(current)
          end
          deleted
        else
          @leafs.delete(current).try &.last
        end
      end

      def empty?
        @leafs.empty? && @sublevels.empty?
      end

      def size
        @leafs.size + @sublevels.values.sum(0, &.size)
      end

      def each(filter : String, &blk : (String, TEntity) -> _)
        # Filters that start with a wildcard never match '$' topics (MQTT-4.7.2-1)
        skip_dollar = filter.starts_with?('#') || filter.starts_with?('+')
        each(StringTokenIterator.new(filter, '/'), skip_dollar, &blk)
      end

      def each(filter : StringTokenIterator, skip_dollar = false, &blk : (String, TEntity) -> _)
        current = filter.next
        if current == "#"
          each_leaf(skip_dollar, &blk)
          @sublevels.each { |key, sublevel| sublevel.each(&blk) unless skip_dollar && key.starts_with?('$') }
          return
        end
        # "#" also matches its parent level, so "a/#" matches the leaf "a" (MQTT 3.1.1 §4.7.1.2)
        rest = filter
        rest_is_hash = rest.next == "#" && !rest.next?
        if current == "+"
          each_plus(filter, rest_is_hash, skip_dollar, &blk)
          return
        end
        if filter.next?
          if rest_is_hash && (leaf = @leafs.fetch(current, nil))
            yield leaf.first, leaf.last
          end
          if sublevel = @sublevels.fetch(current, nil)
            sublevel.each filter, &blk
          end
        elsif leaf = @leafs.fetch(current, nil)
          yield leaf.first, leaf.last
        end
      end

      private def each_leaf(skip_dollar, & : (String, TEntity) -> _)
        @leafs.each { |key, leaf| yield(leaf.first, leaf.last) unless skip_dollar && key.starts_with?('$') }
      end

      private def each_plus(filter : StringTokenIterator, rest_is_hash, skip_dollar, &blk : (String, TEntity) -> _)
        if filter.next?
          each_leaf(skip_dollar, &blk) if rest_is_hash
          @sublevels.each { |key, sublevel| sublevel.each(filter, &blk) unless skip_dollar && key.starts_with?('$') }
        else
          each_leaf(skip_dollar, &blk)
        end
      end

      def each(&blk : (String, TEntity) -> _)
        @leafs.values.each &blk
        @sublevels.values.each(&.each(&blk))
      end

      def inspect
        "#{self.class.name}(@sublevels=#{@sublevels.inspect} @leafs=#{@leafs.inspect})"
      end
    end
  end
end
