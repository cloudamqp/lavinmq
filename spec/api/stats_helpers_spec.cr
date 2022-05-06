require "../spec_helper"
include LavinMQ::HTTP::StatsHelpers

describe LavinMQ::HTTP::StatsHelpers do
  it "should add equal size arrays" do
    a = [1, 2, 3]
    b = [1, 2, 3]
    add_logs!(a, b)
    a.should eq [2, 4, 6]
  end

  it "should add empty a" do
    a = [] of Int32
    b = [1, 2, 3]
    add_logs!(a, b)
    a.should eq [1, 2, 3]
  end

  it "should add empty b" do
    a = [1, 2, 3]
    b = [] of Int32
    add_logs!(a, b)
    a.should eq [1, 2, 3]
  end

  it "should add b shorter than a" do
    a = [1, 2, 3]
    b = [1] of Int32
    add_logs!(a, b)
    a.should eq [1, 2, 4]
  end

  it "should add b shorter than a" do
    a = [1, 2, 3]
    b = [1] of Int32
    add_logs!(a, b)
    a.should eq [1, 2, 4]
  end
end
