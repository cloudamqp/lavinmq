module LavinMQ
  # An immutable hash map: every `put` and `delete` returns a new map that
  # shares all untouched nodes with the old one, so a map, once published,
  # can be read from any thread without locks while a writer builds the next
  # version.
  #
  # It is a CHAMP trie (Steindorfer & Vinju, "Optimizing Hash-Array Mapped
  # Tries for Fast and Lean Immutable JVM Collections"): each node has a
  # bitmap of the 32 hash fragments it holds inline as key/value pairs and a
  # bitmap of those it delegates to child nodes. A write copies only the
  # nodes on the path to the key, about log32(n) small nodes.
  #
  # Each node is a single GC allocation of machine words: a header word (the
  # data bitmap in the low 32 bits, the node bitmap in the high 32), then the
  # key/value pairs, then the child node pointers. Keys and values are stored
  # as one machine word each, so K and V must be represented as a single
  # reference: a class, a union of classes, or a module only classes include.
  # Structs (e.g. `Bytes`, `Set`, integers) and nilable types aren't
  # supported; such maps need their values boxed in a class. The GC scans
  # the block conservatively, which keeps the keys, values and children
  # alive. Keys whose 64-bit hashes are equal share a collision node, created
  # where their hashes are first found equal: header COLLISION, a count, then
  # the pairs.
  #
  # Keys are compared with `#hash` and `#==`, like `Hash`. Lookups and
  # iteration allocate nothing.
  class PersistentMap(K, V)
    include Enumerable({K, V})

    private alias Node = Pointer(UInt64)
    private COLLISION = UInt64::MAX

    getter size : Int32
    @root : Node

    protected def initialize(@root : Node, @size : Int32)
      {% for t in [K, V] %}
        {% for u in t.union_types %}
          {% unless u < Reference || (u.module? && u.includers.all? { |i| i < Reference }) %}
            {% raise "PersistentMap stores keys and values as single references, so it can't hold #{u} (in #{t})" %}
          {% end %}
        {% end %}
      {% end %}
    end

    def self.new : self
      new(new_node(0u32, 0u32, 0, 0), 0)
    end

    def empty? : Bool
      @size == 0
    end

    def []?(key : K) : V?
      n = @root
      h = key.hash
      shift = 0
      loop do
        hdr = n[0]
        if hdr == COLLISION
          n[1].times do |i|
            return collision_value(n, i) if collision_key(n, i) == key
          end
          return
        end
        datamap = hdr.to_u32!
        nodemap = (hdr >> 32).to_u32!
        bit = 1u32 << ((h >> shift) & 31)
        if datamap & bit != 0
          i = index(datamap, bit)
          return key_at(n, i) == key ? value_at(n, i) : nil
        end
        return if nodemap & bit == 0
        n = child_at(n, datamap.popcount, index(nodemap, bit))
        shift += 5
      end
    end

    # Looks up a key by a different representation of it, e.g. a `Bytes` view
    # where the keys are objects owning a copy of the bytes, so the lookup
    # doesn't allocate a key. *hash* must equal `#hash` of the key being
    # looked for, and the block must return true for that key only.
    def find_value(hash h : UInt64, & : K -> Bool) : V?
      n = @root
      shift = 0
      loop do
        hdr = n[0]
        if hdr == COLLISION
          n[1].times do |i|
            return collision_value(n, i) if yield collision_key(n, i)
          end
          return
        end
        datamap = hdr.to_u32!
        nodemap = (hdr >> 32).to_u32!
        bit = 1u32 << ((h >> shift) & 31)
        if datamap & bit != 0
          i = index(datamap, bit)
          return (yield key_at(n, i)) ? value_at(n, i) : nil
        end
        return if nodemap & bit == 0
        n = child_at(n, datamap.popcount, index(nodemap, bit))
        shift += 5
      end
    end

    def [](key : K) : V
      self[key]? || raise KeyError.new("Missing key: #{key.inspect}")
    end

    def has_key?(key : K) : Bool
      !self[key]?.nil?
    end

    # Returns self when the key already maps to this very value
    def put(key : K, value : V) : PersistentMap(K, V)
      root, added = put(@root, key.hash, key, value, 0)
      return self if root == @root
      PersistentMap(K, V).new(root, added ? @size + 1 : @size)
    end

    # Returns self when the key isn't in the map
    def delete(key : K) : PersistentMap(K, V)
      delete_with_value(key)[0]
    end

    # The map without the key, and the value it had (nil, and self, when the
    # key isn't in the map)
    def delete_with_value(key : K) : {PersistentMap(K, V), V?}
      root, removed = delete(@root, key.hash, key, 0)
      return {self, nil} if removed.nil?
      {PersistentMap(K, V).new(root, @size - 1), removed}
    end

    # Depth-first walk with a fixed stack, at most 13 levels plus a collision
    # node, so iterating allocates nothing
    def each(& : {K, V} ->) : Nil
      stack = uninitialized StaticArray(Node, 16)
      next_child = uninitialized StaticArray(Int32, 16)
      depth = 0
      stack[0] = @root
      next_child[0] = -1
      while depth >= 0
        n = stack[depth]
        hdr = n[0]
        if hdr == COLLISION
          n[1].times { |i| yield({collision_key(n, i), collision_value(n, i)}) }
          depth -= 1
          next
        end
        data = hdr.to_u32!.popcount
        if next_child[depth] == -1
          data.times { |i| yield({key_at(n, i), value_at(n, i)}) }
          next_child[depth] = 0
        end
        j = next_child[depth]
        if j < (hdr >> 32).to_u32!.popcount
          next_child[depth] = j + 1
          depth += 1
          stack[depth] = child_at(n, data, j)
          next_child[depth] = -1
        else
          depth -= 1
        end
      end
    end

    def each_key(& : K ->) : Nil
      each { |(k, _)| yield k }
    end

    def each_value(& : V ->) : Nil
      each { |(_, v)| yield v }
    end

    def keys : Array(K)
      Array(K).new(@size).tap { |a| each_key { |k| a << k } }
    end

    def values : Array(V)
      Array(V).new(@size).tap { |a| each_value { |v| a << v } }
    end

    # Pair i of a node lives at words 1 + 2i and 2 + 2i
    @[AlwaysInline]
    private def key_at(n : Node, i) : K
      n[1 + 2 * i].unsafe_as(K)
    end

    @[AlwaysInline]
    private def value_at(n : Node, i) : V
      n[2 + 2 * i].unsafe_as(V)
    end

    # In a collision node word 1 is the count, so pair i is at 2 + 2i
    @[AlwaysInline]
    private def collision_key(n : Node, i) : K
      n[2 + 2 * i].unsafe_as(K)
    end

    @[AlwaysInline]
    private def collision_value(n : Node, i) : V
      n[3 + 2 * i].unsafe_as(V)
    end

    @[AlwaysInline]
    private def child_at(n : Node, data : Int32, j) : Node
      Node.new(n[1 + 2 * data + j])
    end

    @[AlwaysInline]
    private def index(map : UInt32, bit : UInt32) : Int32
      (map & (bit &- 1)).popcount
    end

    @[AlwaysInline]
    protected def self.word(ref) : UInt64
      ref.unsafe_as(UInt64)
    end

    protected def self.alloc(words : Int32) : Node
      GC.malloc(words * sizeof(UInt64)).as(Node)
    end

    protected def self.new_node(datamap : UInt32, nodemap : UInt32, data : Int32, children : Int32) : Node
      n = alloc(1 + 2 * data + children)
      n[0] = datamap.to_u64 | (nodemap.to_u64 << 32)
      n
    end

    private def put(n : Node, h : UInt64, key : K, value : V, shift : Int32) : {Node, Bool}
      hdr = n[0]
      return collision_put(n, h, key, value, shift) if hdr == COLLISION
      datamap = hdr.to_u32!
      nodemap = (hdr >> 32).to_u32!
      data = datamap.popcount
      children = nodemap.popcount
      words = 1 + 2 * data + children
      bit = 1u32 << ((h >> shift) & 31)
      if datamap & bit != 0
        i = index(datamap, bit)
        k0 = key_at(n, i)
        if k0 == key
          return {n, false} if value_at(n, i).same?(value)
          r = self.class.alloc(words)
          r.copy_from(n, words)
          r[2 + 2 * i] = self.class.word(value)
          return {r, false}
        end
        # Two keys share this fragment: push both down into a new child. Like
        # CHAMP, nodes don't store hashes, so the existing key is rehashed;
        # a write-path cost only, lookups never rehash
        sub = merge(k0, value_at(n, i), k0.hash, key, value, h, shift + 5)
        j = index(nodemap, bit)
        r = self.class.new_node(datamap & ~bit, nodemap | bit, data - 1, children + 1)
        (r + 1).copy_from(n + 1, 2 * i)
        (r + 1 + 2 * i).copy_from(n + 3 + 2 * i, 2 * (data - 1 - i))
        rc = r + 1 + 2 * (data - 1)
        nc = n + 1 + 2 * data
        rc.copy_from(nc, j)
        rc[j] = sub.address
        (rc + j + 1).copy_from(nc + j, children - j)
        {r, true}
      elsif nodemap & bit != 0
        j = index(nodemap, bit)
        child = child_at(n, data, j)
        sub, added = put(child, h, key, value, shift + 5)
        return {n, false} if sub == child
        r = self.class.alloc(words)
        r.copy_from(n, words)
        r[1 + 2 * data + j] = sub.address
        {r, added}
      else
        i = index(datamap, bit)
        r = self.class.new_node(datamap | bit, nodemap, data + 1, children)
        (r + 1).copy_from(n + 1, 2 * i)
        r[1 + 2 * i] = self.class.word(key)
        r[2 + 2 * i] = self.class.word(value)
        (r + 3 + 2 * i).copy_from(n + 1 + 2 * i, 2 * (data - i) + children)
        {r, true}
      end
    end

    private def merge(k0 : K, v0 : V, h0 : UInt64, k1 : K, v1 : V, h1 : UInt64, shift : Int32) : Node
      if h0 == h1
        r = self.class.alloc(6)
        r[0] = COLLISION
        r[1] = 2u64
        r[2] = self.class.word(k0)
        r[3] = self.class.word(v0)
        r[4] = self.class.word(k1)
        r[5] = self.class.word(v1)
        return r
      end
      f0 = (h0 >> shift) & 31
      f1 = (h1 >> shift) & 31
      if f0 == f1
        r = self.class.new_node(0u32, 1u32 << f0, 0, 1)
        r[1] = merge(k0, v0, h0, k1, v1, h1, shift + 5).address
      else
        r = self.class.new_node((1u32 << f0) | (1u32 << f1), 0u32, 2, 0)
        a, b = f0 < f1 ? {1, 3} : {3, 1}
        r[a] = self.class.word(k0)
        r[a + 1] = self.class.word(v0)
        r[b] = self.class.word(k1)
        r[b + 1] = self.class.word(v1)
      end
      r
    end

    private def collision_put(n : Node, h : UInt64, key : K, value : V, shift : Int32) : {Node, Bool}
      # Rehashed as nodes don't store hashes (write path only, see put)
      ch = collision_key(n, 0).hash
      return {split(n, ch, key, value, h, shift), true} if ch != h
      count = n[1].to_i
      words = 2 + 2 * count
      count.times do |i|
        next unless collision_key(n, i) == key
        return {n, false} if collision_value(n, i).same?(value)
        r = self.class.alloc(words)
        r.copy_from(n, words)
        r[3 + 2 * i] = self.class.word(value)
        return {r, false}
      end
      r = self.class.alloc(words + 2)
      r.copy_from(n, words)
      r[1] = (count + 1).to_u64
      r[words] = self.class.word(key)
      r[words + 1] = self.class.word(value)
      {r, true}
    end

    # A key whose hash differs from a collision node's: a node holding the
    # key and, as a child, the collision node, below single-child nodes for
    # as long as the two hashes share fragments
    private def split(c : Node, ch : UInt64, key : K, value : V, h : UInt64, shift : Int32) : Node
      fc = (ch >> shift) & 31
      f = (h >> shift) & 31
      if fc == f
        r = self.class.new_node(0u32, 1u32 << fc, 0, 1)
        r[1] = split(c, ch, key, value, h, shift + 5).address
      else
        r = self.class.new_node(1u32 << f, 1u32 << fc, 1, 1)
        r[1] = self.class.word(key)
        r[2] = self.class.word(value)
        r[3] = c.address
      end
      r
    end

    # A node below the root that is left with a single pair and no children
    # is moved up into its parent, which keeps the trie canonical
    private def delete(n : Node, h : UInt64, key : K, shift : Int32) : {Node, V?}
      hdr = n[0]
      return collision_delete(n, h, key, shift) if hdr == COLLISION
      datamap = hdr.to_u32!
      nodemap = (hdr >> 32).to_u32!
      data = datamap.popcount
      children = nodemap.popcount
      words = 1 + 2 * data + children
      bit = 1u32 << ((h >> shift) & 31)
      if datamap & bit != 0
        i = index(datamap, bit)
        return {n, nil} unless key_at(n, i) == key
        r = self.class.new_node(datamap & ~bit, nodemap, data - 1, children)
        (r + 1).copy_from(n + 1, 2 * i)
        (r + 1 + 2 * i).copy_from(n + 3 + 2 * i, 2 * (data - 1 - i) + children)
        {r, value_at(n, i)}
      elsif nodemap & bit != 0
        j = index(nodemap, bit)
        sub, removed = delete(child_at(n, data, j), h, key, shift + 5)
        return {n, nil} if removed.nil?
        shdr = sub[0]
        if shdr != COLLISION && (shdr >> 32) == 0 && shdr.to_u32!.popcount == 1
          i = index(datamap, bit)
          r = self.class.new_node(datamap | bit, nodemap & ~bit, data + 1, children - 1)
          (r + 1).copy_from(n + 1, 2 * i)
          r[1 + 2 * i] = sub[1]
          r[2 + 2 * i] = sub[2]
          (r + 3 + 2 * i).copy_from(n + 1 + 2 * i, 2 * (data - i))
          rc = r + 1 + 2 * (data + 1)
          nc = n + 1 + 2 * data
          rc.copy_from(nc, j)
          (rc + j).copy_from(nc + j + 1, children - j - 1)
          {r, removed}
        else
          r = self.class.alloc(words)
          r.copy_from(n, words)
          r[1 + 2 * data + j] = sub.address
          {r, removed}
        end
      else
        {n, nil}
      end
    end

    private def collision_delete(n : Node, h : UInt64, key : K, shift : Int32) : {Node, V?}
      count = n[1].to_i
      count.times do |i|
        next unless collision_key(n, i) == key
        if count == 2
          # One pair left: a plain one-pair node, which the parent inlines
          other = 1 - i
          r = self.class.new_node(1u32 << ((h >> shift) & 31), 0u32, 1, 0)
          r[1] = n[2 + 2 * other]
          r[2] = n[3 + 2 * other]
          return {r, collision_value(n, i)}
        end
        r = self.class.alloc(2 * count)
        r[0] = COLLISION
        r[1] = (count - 1).to_u64
        (r + 2).copy_from(n + 2, 2 * i)
        (r + 2 + 2 * i).copy_from(n + 4 + 2 * i, 2 * (count - 1 - i))
        return {r, collision_value(n, i)}
      end
      {n, nil}
    end
  end

  # A map that can be read from any thread without locks. Reads use the
  # current `PersistentMap` version; writes create the next version and
  # publish it with a compare-and-set, retrying if another writer published
  # first, so concurrent writers never lose each other's updates. Every read,
  # including each `Enumerable` method, uses one version throughout.
  class CowMap(K, V)
    include Enumerable({K, V})

    def initialize(map = PersistentMap(K, V).new)
      @map = Atomic(PersistentMap(K, V)).new(map)
    end

    # The current version. Iterating it sees a consistent snapshot, however
    # long the iteration takes and whatever writers do meanwhile.
    def snapshot : PersistentMap(K, V)
      @map.get(:acquire)
    end

    delegate :[]?, :[], :has_key?, :size, :empty?, :keys, :values, :each_key, :each_value, to: snapshot

    def each(& : {K, V} ->) : Nil
      snapshot.each { |kv| yield kv }
    end

    def []=(key : K, value : V) : V
      loop do
        old = snapshot
        map = old.put(key, value)
        return value if map.same?(old) || @map.compare_and_set(old, map, :release, :acquire)[1]
      end
    end

    def delete(key : K) : V?
      loop do
        old = snapshot
        map, value = old.delete_with_value(key)
        return if value.nil?
        return value if @map.compare_and_set(old, map, :release, :acquire)[1]
      end
    end

    def clear : Nil
      @map.set(PersistentMap(K, V).new, :release)
    end
  end
end
