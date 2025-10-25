require "./spec_helper"

describe LavinMQ::RingBuffer do
  describe "initialization" do
    it "rounds capacity up to power of 2" do
      rb = LavinMQ::RingBuffer(Int32).new(5)
      rb.size.should eq 0
      8.times { |i| rb.push(i) }
      rb.size.should eq 8
    end

    it "handles power of 2 capacity" do
      rb = LavinMQ::RingBuffer(Int32).new(8)
      8.times { |i| rb.push(i) }
      rb.size.should eq 8
    end

    it "handles minimum capacity of 2" do
      rb = LavinMQ::RingBuffer(Int32).new(2)
      rb.push(10)
      rb.push(20)
      rb.size.should eq 2
      rb[0].should eq 10
      rb[1].should eq 20
    end

    it "raises ArgumentError for capacity less than 2" do
      expect_raises(ArgumentError, "Capacity must be at least 2") do
        LavinMQ::RingBuffer(Int32).new(1)
      end

      expect_raises(ArgumentError, "Capacity must be at least 2") do
        LavinMQ::RingBuffer(Int32).new(0)
      end
    end
  end

  describe "push and indexing" do
    it "pushes values when not full" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(1)
      rb.push(2)
      rb.push(3)

      rb.size.should eq 3
      rb[0].should eq 1
      rb[1].should eq 2
      rb[2].should eq 3
    end

    it "maintains size when at capacity" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      5.times { |i| rb.push(i) }

      rb.size.should eq 4
    end

    it "overwrites oldest values when full" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(0)
      rb.push(1)
      rb.push(2)
      rb.push(3)
      rb.push(4) # Overwrites 0

      rb.size.should eq 4
      rb[0].should eq 1
      rb[1].should eq 2
      rb[2].should eq 3
      rb[3].should eq 4
    end
  end

  describe "wrapping behavior" do
    it "wraps correctly when buffer is full" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      8.times { |i| rb.push(i) }

      rb.size.should eq 4
      rb[0].should eq 4
      rb[1].should eq 5
      rb[2].should eq 6
      rb[3].should eq 7
    end

    it "wraps multiple times" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      20.times { |i| rb.push(i) }

      rb.size.should eq 4
      rb[0].should eq 16
      rb[1].should eq 17
      rb[2].should eq 18
      rb[3].should eq 19
    end

    it "handles partial wrap correctly" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      5.times { |i| rb.push(i) }

      rb.size.should eq 4
      rb[0].should eq 1
      rb[1].should eq 2
      rb[2].should eq 3
      rb[3].should eq 4
    end
  end

  describe "index access errors" do
    it "raises IndexError for negative index" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(1)

      expect_raises(IndexError) do
        rb[-1]
      end
    end

    it "raises IndexError for index >= size" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(1)
      rb.push(2)

      expect_raises(IndexError) do
        rb[2]
      end
    end

    it "raises IndexError on empty buffer" do
      rb = LavinMQ::RingBuffer(Int32).new(4)

      expect_raises(IndexError) do
        rb[0]
      end
    end
  end

  describe "to_a" do
    it "returns empty array when empty" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.to_a.should eq [] of Int32
    end

    it "returns all elements before wrapping" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(1)
      rb.push(2)
      rb.push(3)

      rb.to_a.should eq [1, 2, 3]
    end

    it "returns elements in correct order at capacity" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      rb.push(0)
      rb.push(1)
      rb.push(2)
      rb.push(3)

      rb.to_a.should eq [0, 1, 2, 3]
    end

    it "returns elements in correct order after wrapping" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      5.times { |i| rb.push(i) }

      rb.to_a.should eq [1, 2, 3, 4]
    end

    it "returns elements in correct order after multiple wraps" do
      rb = LavinMQ::RingBuffer(Int32).new(4)
      10.times { |i| rb.push(i) }

      rb.to_a.should eq [6, 7, 8, 9]
    end

    it "handles edge case: full buffer where tail equals head" do
      # This tests the case where @size == @capacity and tail == @head
      # After exactly @capacity pushes, tail wraps around to equal @head
      rb = LavinMQ::RingBuffer(Int32).new(4)
      4.times { |i| rb.push(i) }

      # At this point: @size == 4, @head == 0, tail == 0
      # Condition: @size < @capacity (false) || tail > @head (false)
      # Should use two-segment copy path
      rb.to_a.should eq [0, 1, 2, 3]
      rb.size.should eq 4
    end

    it "handles edge case: full buffer after exact capacity wraps" do
      # Fill buffer, then add exactly capacity more to wrap completely
      rb = LavinMQ::RingBuffer(Int32).new(4)
      8.times { |i| rb.push(i) }

      # @head == 0, tail == 0, @size == 4 (wrapped exactly once)
      rb.to_a.should eq [4, 5, 6, 7]
      rb.size.should eq 4
    end
  end

  describe "different types" do
    it "works with strings" do
      rb = LavinMQ::RingBuffer(String).new(3)
      rb.push("a")
      rb.push("b")
      rb.push("c")
      rb.push("d")
      rb.push("e")

      rb.to_a.should eq ["b", "c", "d", "e"]
    end

    it "works with custom structs" do
      rb = LavinMQ::RingBuffer(NamedTuple(id: Int32, name: String)).new(2)
      rb.push({id: 1, name: "first"})
      rb.push({id: 2, name: "second"})
      rb.push({id: 3, name: "third"})

      rb[0].should eq({id: 2, name: "second"})
      rb[1].should eq({id: 3, name: "third"})
    end
  end
end
