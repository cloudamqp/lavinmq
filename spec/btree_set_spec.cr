require "spec"
require "../src/lavinmq/btree_set"

# A key with the shape of a SegmentPosition: ordered by two fields, 16 bytes
private record Pos, segment : UInt32, position : UInt32, padding : UInt64 = 0u64 do
  include Comparable(self)

  def <=>(other : self)
    r = segment <=> other.segment
    r.zero? ? position <=> other.position : r
  end
end

# Walks the whole tree and raises on a broken invariant
class LavinMQ::BTreeSet(T)
  def check_invariants : Nil
    count, _depth = check_node(@root, nil, nil, 0)
    raise "size #{@size} but #{count} keys" unless count == @size
    raise "first leaf is not the leftmost" unless @first_leaf.same?(leftmost(@root))
  end

  # Every key below `node` must lie in `lo...hi`; returns the key count and leaf depth
  private def check_node(node : Node(T), lo : T?, hi : T?, depth : Int32) : {Int32, Int32}
    check_keys(node, lo, hi)
    return {node.size, depth} if node.is_a?(Leaf(T))
    node = node.as(Internal(T))
    count = 0
    leaf_depth = 0
    (0..node.size).each do |i|
      child_lo = i.zero? ? lo : node.keys[i - 1]
      child_hi = i == node.size ? hi : node.keys[i]
      c, d = check_node(node.children[i], child_lo, child_hi, depth + 1)
      raise "leaves at different depths" if i > 0 && d != leaf_depth
      leaf_depth = d
      count += c
    end
    {count, leaf_depth}
  end

  private def check_keys(node : Node(T), lo : T?, hi : T?) : Nil
    raise "node over capacity" if node.size > @cap
    raise "empty node below the root" if node.size.zero? && !node.same?(@root)
    (1...node.size).each do |i|
      raise "keys out of order" unless (node.keys[i - 1] <=> node.keys[i]) < 0
    end
    return if node.size.zero?
    raise "key below its separator" if lo && (node.keys[0] <=> lo) < 0
    raise "key not below its separator" if hi && (node.keys[node.size - 1] <=> hi) >= 0
  end
end

describe LavinMQ::BTreeSet do
  it "is empty when created" do
    set = LavinMQ::BTreeSet(Int32).new
    set.size.should eq 0
    set.empty?.should be_true
    set.first?.should be_nil
    set.shift?.should be_nil
    set.to_a.should be_empty
  end

  it "keeps keys sorted and unique" do
    set = LavinMQ::BTreeSet(Int32).new
    [5, 1, 4, 1, 9, 2, 6, 3, 5, 0].each { |i| set.insert i }
    set.size.should eq 8
    set.to_a.should eq [0, 1, 2, 3, 4, 5, 6, 9]
    set.first?.should eq 0
  end

  it "tells an insert of a present key from a new one" do
    set = LavinMQ::BTreeSet(Int32).new
    set.insert(1).should be_true
    set.insert(1).should be_false
    set.includes?(1).should be_true
    set.includes?(2).should be_false
  end

  it "shifts in ascending order" do
    set = LavinMQ::BTreeSet(Int32).new
    (1..1000).to_a.shuffle(Random.new(3)).each { |i| set.insert i }
    shifted = Array(Int32).new
    while i = set.shift?
      shifted << i
    end
    shifted.should eq (1..1000).to_a
    set.empty?.should be_true
    set.first?.should be_nil
  end

  it "deletes keys anywhere and reports whether they were present" do
    set = LavinMQ::BTreeSet(Int32).new
    (0...5000).each { |i| set.insert i }
    (0...5000).step(3).each { |i| set.delete(i).should be_true }
    set.delete(3).should be_false
    set.size.should eq 5000 - 1667
    set.first?.should eq 1
    set.to_a.should eq (0...5000).reject { |i| i % 3 == 0 }
  end

  it "can be cleared and reused" do
    set = LavinMQ::BTreeSet(Int32).new
    (0...3000).each { |i| set.insert i }
    set.clear
    set.size.should eq 0
    set.first?.should be_nil
    set.insert(7).should be_true
    set.first?.should eq 7
  end

  it "shrinks back to nothing when drained from the front" do
    set = LavinMQ::BTreeSet(Int32).new
    (0...10_000).each { |i| set.insert i }
    5000.times { set.shift? }
    set.first?.should eq 5000
    set.size.should eq 5000
    (0...5000).each { |i| set.includes?(i).should be_false }
    set.check_invariants
    5000.times { set.shift? }
    set.empty?.should be_true
    set.check_invariants
    set.insert(1).should be_true
    set.shift?.should eq 1
  end

  it "shrinks back to nothing when drained from the back" do
    set = LavinMQ::BTreeSet(Int32).new
    (0...10_000).each { |i| set.insert i }
    9999.downto(5000) { |i| set.delete(i).should be_true }
    set.check_invariants
    set.size.should eq 5000
    set.first?.should eq 0
    4999.downto(0) { |i| set.delete(i).should be_true }
    set.check_invariants
    set.empty?.should be_true
  end

  it "orders struct keys by their own comparison" do
    set = LavinMQ::BTreeSet(Pos).new
    set.insert Pos.new(2, 1)
    set.insert Pos.new(1, 9)
    set.insert Pos.new(1, 2)
    set.insert Pos.new(1, 2)
    set.size.should eq 3
    set.map { |p| {p.segment, p.position} }.should eq [{1, 2}, {1, 9}, {2, 1}]
  end

  # Keys come from a range small enough that inserts hit present keys and
  # deletes hit missing ones, over enough rounds to split and empty every path
  it "agrees with a sorted array under random inserts, deletes and shifts" do
    rand = Random.new(2026)
    set = LavinMQ::BTreeSet(Pos).new
    model = Set(Pos).new
    40_000.times do |round|
      key = Pos.new(rand.rand(1u32..3u32), rand.rand(1u32..3000u32))
      case rand.rand(10)
      when 0..4
        set.insert(key).should eq model.add?(key)
      when 5..6
        set.delete(key).should eq model.delete(key)
      else
        min = model.min?
        model.delete(min) if min
        set.shift?.should eq min
      end
      set.size.should eq model.size
      set.first?.should eq model.min?
      set.includes?(key).should eq model.includes?(key)
      if round % 100 == 0
        set.check_invariants
        set.to_a.should eq model.to_a.sort if round % 10_000 == 0
      end
    end
    set.check_invariants
    set.to_a.should eq model.to_a.sort
  end
end
