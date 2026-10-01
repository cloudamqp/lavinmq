require "spec"
require "../../src/stdlib/hash_shrunk"

describe "Hash#shrunk" do
  it "returns a right sized copy with the same entries" do
    original = Hash(Int32, String).new { |_, k| k.to_s }
    1000.times { |i| original[i] = i.to_s }
    (10...1000).each { |i| original.delete i }
    h = original.shrunk
    h.should_not be original
    h.size.should eq 10
    h.keys.should eq (0...10).to_a
    h[5].should eq "5"
    h[2000].should eq "2000" # default block is kept
    100.times { |i| h[i] = "x" }
    h.size.should eq 100
  end

  it "keeps compare_by_identity" do
    h = Hash(String, Int32).new.compare_by_identity
    keys = Array.new(1000, &.to_s)
    keys.each_with_index { |k, i| h[k] = i }
    keys[1..].each { |k| h.delete k }
    h = h.shrunk
    h.compare_by_identity?.should be_true
    h[keys[0]]?.should eq 0
    h["0"]?.should be_nil
  end

  it "returns self when not mostly empty" do
    h = {1 => 2}
    h.shrunk.should be h
  end

  it "leaves a hash being iterated intact" do
    h = Hash(Int32, Int32).new
    1000.times { |i| h[i] = i }
    (10...1000).each { |i| h.delete i }
    seen = [] of Int32
    h.each do |k, _|
      h = h.shrunk if k == 0
      seen << k
    end
    seen.should eq (0...10).to_a
  end
end
