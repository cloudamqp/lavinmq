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

    it "will read until buffer contains at least size bytes" do
      initial_data = Bytes.new(3)
      Random::Secure.random_bytes(initial_data)
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 100
        wg = WaitGroup.new(1)
        spawn do
          read_io.peek(20)
          wg.done
        end
        Fiber.yield
        read_io.peek.size.should eq initial_data.size
        extra_data = Bytes.new(17)
        Random::Secure.random_bytes(extra_data)
        write_io.write extra_data
        wg.wait
        read_io.peek.size.should eq(initial_data.size + extra_data.size)
      end
    end

    it "will read up to buffer size" do
      initial_data = Bytes.new(3)
      Random::Secure.random_bytes(initial_data)
      with_io(initial_data) do |read_io, write_io|
        read_io.read_buffering = true
        read_io.buffer_size = 200
        wg = WaitGroup.new(1)
        spawn do
          read_io.peek(20)
          wg.done
        end
        Fiber.yield
        read_io.peek.size.should eq initial_data.size
        extra_data = Bytes.new(500)
        Random::Secure.random_bytes(extra_data)
        write_io.write extra_data
        wg.wait
        read_io.peek.size.should eq(read_io.buffer_size)
      end
    end
  end
end
