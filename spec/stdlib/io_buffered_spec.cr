require "spec"
require "../../src/stdlib/io_buffered"

describe IO::BufferPool do
  it "reuses released buffers" do
    pool = IO::BufferPool.new(1024)
    buf = pool.acquire
    pool.release(buf)
    pool.acquire.should eq buf
    pool.stats[:allocated].should eq 1
    pool.stats[:reused].should eq 1
  end
end

describe IO::Buffered do
  describe "with buffer pool" do
    it "reads data larger than the buffer that arrived in one burst" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      data = Bytes.new(10_000, &.to_u8!)
      writer.write data
      writer.flush # but keep it open, no more data or edges will arrive
      done = Channel(Bytes).new
      spawn do
        buf = Bytes.new(data.size)
        # small reads go via the read buffer
        data.size.times { |i| reader.read(buf[i, 1]) }
        done.send buf
      end
      select
      when buf = done.receive
        buf.should eq data
      when timeout(2.seconds)
        fail "read timed out"
      end
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "doesn't hold a read buffer while waiting for data" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      read = Channel(UInt8).new
      spawn { read.send reader.read_byte.not_nil! }
      Fiber.yield
      reader.@in_buffer.null?.should be_true
      writer.write_byte 7_u8
      writer.flush
      read.receive.should eq 7_u8
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "releases the read buffer when all buffered data is consumed" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      writer.write Bytes.new(10, 1_u8)
      writer.flush
      buf = Bytes.new(5)
      reader.read_fully(buf)
      reader.@in_buffer.null?.should be_false
      reader.read_fully(buf)
      reader.@in_buffer.null?.should be_true
      pool.available.should eq 1
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "doesn't return a buffer to the pool when closed during a blocked flush" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      writes = 0
      closing = false
      done = Channel(Nil).new
      spawn do
        until closing
          writer.write Bytes.new(512)
          writer.flush # eventually blocks holding the pooled buffer
          writes += 1
        end
      rescue IO::Error
      ensure
        done.send nil
      end
      loop do # until the writer is blocked
        prev = writes
        sleep 20.milliseconds
        break if writes == prev
      end
      closing = true
      spawn { writer.close } # flushes concurrently without a lock, like Client#close_socket
      Fiber.yield
      reader.gets_to_end
      done.receive
      acquired = pool.stats[:allocated] + pool.stats[:reused]
      # the buffer that was in use when the socket was closed is left to the GC
      pool.stats[:released].should eq acquired - 1
    ensure
      reader.try &.close
    end

    it "releases the write buffer after flush" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      writer.write Bytes.new(10, 1_u8)
      writer.@out_buffer.null?.should be_false
      writer.flush
      writer.@out_buffer.null?.should be_true
      pool.available.should eq 1
      buf = Bytes.new(10)
      reader.read_fully(buf)
      buf.should eq Bytes.new(10, 1_u8)
      writer.write Bytes.new(10, 2_u8)
      writer.flush
      reader.read_fully(buf)
      buf.should eq Bytes.new(10, 2_u8)
      pool.stats[:allocated].should eq 1
      pool.stats[:reused].should eq 1
    ensure
      reader.try &.close
      writer.try &.close
    end
  end
end
