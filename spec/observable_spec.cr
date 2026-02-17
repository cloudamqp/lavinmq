require "spec"

describe "Callback pattern" do
  it "registers and notifies typed callbacks" do
    calls = [] of String
    arr = Array(Proc(String, Nil)).new
    arr << ->(name : String) { calls << name; nil }

    arr.each &.call("hello")
    calls.should eq ["hello"]
  end

  it "removes callbacks" do
    calls = 0
    arr = Array(Proc(Nil)).new
    cb = -> { calls += 1; nil }
    arr << cb

    arr.each &.call
    calls.should eq 1

    arr.delete(cb)
    arr.each &.call
    calls.should eq 1
  end

  it "supports multiple callbacks" do
    calls1 = [] of String
    calls2 = [] of String
    arr = Array(Proc(String, Nil)).new
    arr << ->(s : String) { calls1 << s; nil }
    arr << ->(s : String) { calls2 << s; nil }

    arr.each &.call("a")
    arr.each &.call("b")

    calls1.should eq ["a", "b"]
    calls2.should eq ["a", "b"]
  end
end
