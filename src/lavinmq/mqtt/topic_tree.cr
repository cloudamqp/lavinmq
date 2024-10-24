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
        current = topic.next.not_nil!
        if topic.next?
          @sublevels[current].insert(topic, entity)
        else
          old_value = @leafs[current]?
          @leafs[current] = {topic.to_s, entity}
          old_value.try &.last
        end
      end

      def []?(topic : String) : (TEntity | Nil)
        self[StringTokenIterator.new(topic, '/')]?
      end

      def []?(topic : StringTokenIterator) : (TEntity | Nil)
        current = topic.next
        if topic.next?
          return unless @sublevels.has_key?(current)
          @sublevels[current][topic]?
        else
          @leafs[current]?.try &.last
        end
      end

      def [](topic : String) : TEntity
        self[StringTokenIterator.new(topic, '/')]
      rescue KeyError
        raise KeyError.new "#{topic} not found"
      end

      def [](topic : StringTokenIterator) : TEntity
        current = topic.next
        if topic.next?
          raise KeyError.new unless @sublevels.has_key?(current)
          @sublevels[current][topic]
        else
          @leafs[current].last
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
        each(StringTokenIterator.new(filter, '/'), &blk)
      end

      def each(filter : StringTokenIterator, &blk : (String, TEntity) -> _)
        current = filter.next
        if current == "#"
          each &blk
          return
        end
        if current == "+"
          if filter.next?
            @sublevels.values.each(&.each(filter, &blk))
          else
            @leafs.values.each &blk
          end
          return
        end
        if filter.next?
          if sublevel = @sublevels.fetch(current, nil)
            sublevel.each filter, &blk
          end
        else
          if leaf = @leafs.fetch(current, nil)
            yield leaf.first, leaf.last
          end
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
