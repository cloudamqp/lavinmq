module LavinMQ
  # An ordered set of `T`, which must define `<=>`, kept as a B+tree.
  #
  # Keys live in leaves of a few dozen keys, searched by a linear scan, with
  # separators in the internal nodes above. The smallest key is read and
  # removed in constant time from the leftmost leaf, which is remembered.
  # Nodes are not rebalanced on removal, only unlinked once empty, so the tree
  # stays compact when drained from the front.
  class BTreeSet(T)
    include Enumerable(T)

    # Fills a Boehm size class: 1024 less its header
    NODE_BYTES = 1008

    getter size = 0

    @cap : Int32
    @root : Node(T)
    @first_leaf : Leaf(T)

    def initialize
      @cap = (NODE_BYTES // sizeof(T) - 1).clamp(8, 64)
      @root = @first_leaf = Leaf(T).new(@cap)
    end

    def empty? : Bool
      @size.zero?
    end

    # Adds `key` and returns true, or returns false if an equal key is present
    def insert(key : T) : Bool
      before = @size
      if split = insert_into(@root, key)
        root = Internal(T).new(@cap)
        root.children[0] = @root
        root.keys[0] = split.key
        root.children[1] = split.node
        root.size = 1
        @root = root
      end
      @size != before
    end

    # Removes `key` and returns true, or returns false if it is not present
    def delete(key : T) : Bool
      before = @size
      @root = delete_from(@root, key) || @root
      return false if @size == before
      @first_leaf = leftmost(@root) if @first_leaf.size.zero?
      true
    end

    def includes?(key : T) : Bool
      node = @root
      while node.is_a?(Internal(T))
        node = node.children[node.upper_bound(key)]
      end
      i = node.lower_bound(key)
      i < node.size && (node.keys[i] <=> key).zero?
    end

    def first? : T?
      @first_leaf.keys[0] unless @first_leaf.size.zero?
    end

    def shift? : T?
      leaf = @first_leaf
      return if leaf.size.zero?
      key = leaf.keys[0]
      if leaf.size > 1
        leaf.remove_at(0)
        @size -= 1
      else
        delete(key)
      end
      key
    end

    def clear : Nil
      @root = @first_leaf = Leaf(T).new(@cap)
      @size = 0
    end

    def each(& : T ->) : Nil
      node = @root
      path = Array({Internal(T), Int32}).new
      loop do
        while node.is_a?(Internal(T))
          path << {node, 0}
          node = node.children[0]
        end
        leaf = node.as(Leaf(T))
        leaf.size.times { |i| yield leaf.keys[i] }
        loop do
          parent, i = path.pop? || return
          next unless i < parent.size
          path << {parent, i + 1}
          node = parent.children[i + 1]
          break
        end
      end
    end

    private def leftmost(node : Node(T)) : Leaf(T)
      while node.is_a?(Internal(T))
        node = node.children[0]
      end
      node.as(Leaf(T))
    end

    private record Split(T), key : T, node : Node(T)

    # Returns the separator and new right sibling when `node` had to split.
    # A node splits in half, except at an append, where only the appended
    # key moves: keys arriving in order then fill every node.
    private def insert_into(node : Node(T), key : T) : Split(T)?
      case node
      when Leaf(T)
        i = node.lower_bound(key)
        return if i < node.size && (node.keys[i] <=> key).zero?
        node.insert_at(i, key)
        @size += 1
        at = i == node.size - 1 ? i : node.size // 2
      when Internal(T)
        i = node.upper_bound(key)
        split = insert_into(node.children[i], key) || return
        node.insert_at(i, split.key, split.node)
        at = i == node.size - 1 ? i - 1 : node.size // 2
      else raise "BUG: unknown node"
      end
      node.split(at) if node.size > @cap
    end

    # Returns what stands in `node`'s place: itself, its sole child, or nil
    private def delete_from(node : Node(T), key : T) : Node(T)?
      case node
      when Leaf(T)
        i = node.lower_bound(key)
        return node unless i < node.size && (node.keys[i] <=> key).zero?
        node.remove_at(i)
        @size -= 1
        node unless node.size.zero?
      when Internal(T)
        i = node.upper_bound(key)
        child = node.children[i]
        replacement = delete_from(child, key)
        return node if replacement.same?(child)
        if replacement
          node.children[i] = replacement
          return node
        end
        node.remove_child(i)
        node.size.zero? ? node.children[0] : node
      else raise "BUG: unknown node"
      end
    end

    private abstract class Node(T)
      getter keys : Pointer(T)
      property size = 0

      # One slot more than the capacity: a node takes an insert, then splits
      def initialize(@cap : Int32)
        @keys = Pointer(T).malloc(@cap + 1)
      end

      # Index of the first key not less than `key`, or `size`
      def lower_bound(key : T) : Int32
        return @size if @size > 0 && (@keys[@size - 1] <=> key) < 0
        i = 0
        while i < @size && (@keys[i] <=> key) < 0
          i += 1
        end
        i
      end

      # Index of the first key greater than `key`, or `size`
      def upper_bound(key : T) : Int32
        return @size if @size > 0 && (@keys[@size - 1] <=> key) <= 0
        i = 0
        while i < @size && (@keys[i] <=> key) <= 0
          i += 1
        end
        i
      end

      abstract def split(at : Int32) : Split(T)
    end

    private class Leaf(T) < Node(T)
      def insert_at(i : Int32, key : T) : Nil
        (@keys + i + 1).move_from(@keys + i, @size - i)
        @keys[i] = key
        @size += 1
      end

      def remove_at(i : Int32) : Nil
        (@keys + i).move_from(@keys + i + 1, @size - i - 1)
        @size -= 1
      end

      # Moves the keys from `at` on to a new leaf; its first key separates
      def split(at : Int32) : Split(T)
        right = Leaf(T).new(@cap)
        right.keys.copy_from(@keys + at, @size - at)
        right.size = @size - at
        @size = at
        Split(T).new(right.keys[0], right)
      end
    end

    private class Internal(T) < Node(T)
      # Child `i` holds the keys between `keys[i - 1]` and `keys[i]`
      getter children : Pointer(Node(T))

      def initialize(cap : Int32)
        super
        @children = Pointer(Node(T)).malloc(@cap + 2)
      end

      def insert_at(i : Int32, key : T, child : Node(T)) : Nil
        (@keys + i + 1).move_from(@keys + i, @size - i)
        (@children + i + 2).move_from(@children + i + 1, @size - i)
        @keys[i] = key
        @children[i + 1] = child
        @size += 1
      end

      # Drops the separator to the child's left, or the first child's right
      def remove_child(i : Int32) : Nil
        k = i.zero? ? 0 : i - 1
        (@keys + k).move_from(@keys + k + 1, @size - k - 1)
        (@children + i).move_from(@children + i + 1, @size - i)
        @size -= 1
      end

      # Moves the keys after `at` to a new node; the key at `at` moves up
      def split(at : Int32) : Split(T)
        right = Internal(T).new(@cap)
        right.keys.copy_from(@keys + at + 1, @size - at - 1)
        right.children.copy_from(@children + at + 1, @size - at)
        right.size = @size - at - 1
        @size = at
        Split(T).new(@keys[at], right)
      end
    end
  end
end
