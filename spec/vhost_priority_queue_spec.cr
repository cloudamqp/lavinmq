require "./spec_helper"

describe LavinMQ::VHost::PriorityQueue do
  it "sorts entries" do
    pq = LavinMQ::VHost::PriorityQueue(Int32).new(3)
    3.times do
      [3, 1, 2].shuffle!.each do |i|
        pq << i
      end
      pq.shift.should eq 1
      pq.shift.should eq 2
      pq.shift.should eq 3
    end
  end
end
