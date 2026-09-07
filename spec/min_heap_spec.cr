require "spec"
require "../src/lavinmq/min_heap"

describe LavinMQ::MinHeap do
  it "is empty when created" do
    heap = LavinMQ::MinHeap(Int32).new
    heap.size.should eq 0
    heap.empty?.should be_true
    heap.first?.should be_nil
    heap.shift?.should be_nil
  end

  it "returns the smallest element without removing it" do
    heap = LavinMQ::MinHeap(Int32).new
    heap.push 5
    heap.first?.should eq 5
    heap.push 3
    heap.first?.should eq 3
    heap.first?.should eq 3
    heap.size.should eq 2
  end

  it "shifts in ascending order" do
    heap = LavinMQ::MinHeap(Int32).new
    [5, 1, 4, 1, 9, 2, 6, 3, 5, 0].each { |i| heap.push i }
    shifted = Array(Int32).new
    while i = heap.shift?
      shifted << i
    end
    shifted.should eq [0, 1, 1, 2, 3, 4, 5, 5, 6, 9]
  end

  it "shifts in ascending order for randomized input" do
    heap = LavinMQ::MinHeap(Int32).new
    input = Array(Int32).new(1000) { Random.rand(10_000) }
    input.each { |i| heap.push i }
    heap.size.should eq input.size
    shifted = Array(Int32).new
    while i = heap.shift?
      shifted << i
    end
    shifted.should eq input.sort
  end

  it "keeps the heap valid when pushes and shifts are interleaved" do
    heap = LavinMQ::MinHeap(Int32).new
    reference = Array(Int32).new
    500.times do
      if reference.empty? || Random.rand(2).zero?
        v = Random.rand(1000)
        heap.push v
        reference << v
      else
        reference.sort!
        heap.shift?.should eq reference.shift
      end
      heap.size.should eq reference.size
      heap.first?.should eq reference.min unless reference.empty?
    end
  end

  it "shifts the only element" do
    heap = LavinMQ::MinHeap(Int32).new
    heap.push 42
    heap.shift?.should eq 42
    heap.empty?.should be_true
    heap.shift?.should be_nil
  end

  it "handles already sorted and reverse sorted input" do
    ascending = LavinMQ::MinHeap(Int32).new
    descending = LavinMQ::MinHeap(Int32).new
    (1..100).each { |i| ascending.push i }
    100.downto(1) { |i| descending.push i }
    (1..100).each do |i|
      ascending.shift?.should eq i
      descending.shift?.should eq i
    end
  end

  it "empties on clear and is reusable afterwards" do
    heap = LavinMQ::MinHeap(Int32).new
    [3, 1, 2].each { |i| heap.push i }
    heap.clear
    heap.size.should eq 0
    heap.empty?.should be_true
    heap.first?.should be_nil
    heap.shift?.should be_nil
    heap.push 7
    heap.first?.should eq 7
  end

  it "orders any Comparable" do
    heap = LavinMQ::MinHeap(String).new
    ["pear", "apple", "orange"].each { |s| heap.push s }
    heap.shift?.should eq "apple"
    heap.shift?.should eq "orange"
    heap.shift?.should eq "pear"
  end
end
