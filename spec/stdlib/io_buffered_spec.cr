require "random"
require "spec"
require "socket"
require "wait_group"
require "../../src/stdlib/io_buffered"

def with_io(initial_content = "foo bar baz".to_slice, &)
  read_io, write_io = UNIXSocket.pair
  write_io.write initial_content
  yield read_io, write_io
ensure
  read_io.try &.close
  write_io.try &.close
end

describe IO::Buffered do
  describe "#peek" do
    it "raises if read_buffering is false" do
      with_io do |read_io, _write_io|
        read_io.read_buffering = false
        expect_raises(RuntimeError) do
          read_io.peek(5)
        end
      end
    end

    it "raises if size is greater than buffer_size" do
      with_io do |read_io, _write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 5
        expect_raises(ArgumentError) do
          read_io.peek(10)
        end
      end
    end

    it "raises unless size is positive" do
      with_io do |read_io, _write_io|
        read_io.read_buffering = true
        expect_raises(ArgumentError) do
          read_io.peek(-10)
        end
      end
    end

    it "returns slice of requested size" do
      with_io("foo bar".to_slice) do |read_io, _write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 5
        read_io.peek(3).should eq "foo".to_slice
      end
    end

    it "will read until buffer contains at least size bytes" do
      initial_data = "foo".to_slice
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 10

        read_io.peek.should eq "foo".to_slice

        extra_data = "barbaz".to_slice
        write_io.write extra_data

        peeked = read_io.peek(6)
        peeked.should eq "foobar".to_slice
      end
    end

    it "will read up to buffer size if possible" do
      initial_data = "foo".to_slice
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 9

        read_io.peek.should eq "foo".to_slice

        extra_data = "barbaz".to_slice
        write_io.write extra_data

        peeked = read_io.peek(6)
        peeked.should eq "foobar".to_slice
      end
    end

    it "will move existing data to beginning of internal buffer " do
      initial_data = "000foo".to_slice
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 9

        data = Bytes.new(3)
        read_io.read data
        data.should eq "000".to_slice

        extra_data = "barbaz".to_slice
        write_io.write extra_data

        peeked = read_io.peek(6)
        peeked.should eq "foobar".to_slice
      end
    end

    it "returns what's in buffer upto size if io is closed" do
      initial_data = "foobar".to_slice
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 9

        data = Bytes.new(3)
        read_io.read data
        data.should eq "foo".to_slice

        write_io.close

        read_io.peek(6).should eq "bar".to_slice
      end
    end
  end
end
