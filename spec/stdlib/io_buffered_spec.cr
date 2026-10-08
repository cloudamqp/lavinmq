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

  it "reuses the most recently released buffer first" do
    pool = IO::BufferPool.new(1024)
    a = pool.acquire
    b = pool.acquire
    pool.release(a)
    pool.release(b)
    pool.acquire.should eq b
  end

  it "keeps at most MAX_PER_THREAD buffers per thread" do
    pool = IO::BufferPool.new(1024)
    extra = 5
    buffers = Array.new(IO::BufferPool::MAX_PER_THREAD + extra) { pool.acquire }
    buffers.each { |b| pool.release(b) }
    stats = pool.stats
    stats[:available].should eq IO::BufferPool::MAX_PER_THREAD
    stats[:released].should eq IO::BufferPool::MAX_PER_THREAD
    stats[:dropped].should eq extra
  end

  it "caches buffers per thread" do
    pool = IO::BufferPool.new(1024)
    buf = pool.acquire
    Fiber::ExecutionContext::Isolated.new("release on another thread") do
      pool.release(buf)
    end.wait
    # released into the other thread's cache, not this one's
    pool.acquire.should_not eq buf
    stats = pool.stats
    stats[:threads].should eq 2
    stats[:available].should eq 1
    stats[:allocated].should eq 2
  end

  it "keeps reusing buffers of pools for other buffer sizes" do
    old_pool = IO::BufferPool.for(1111)
    old_buf = old_pool.acquire
    new_pool = IO::BufferPool.for(2222)
    new_pool.should_not be old_pool
    # established connections still use the old pool, after a config reload
    old_pool.release(old_buf)
    old_pool.acquire.should eq old_buf
    old_pool.stats[:dropped].should eq 0
    new_buf = new_pool.acquire
    new_pool.release(new_buf)
    new_pool.stats[:available].should eq 1
    IO::BufferPool.for(1111).should be old_pool
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
      wrote = Channel(Nil).new(1)
      # can block, macOS has a small UNIX socket send buffer
      spawn do
        writer.write data
        writer.flush # but keep it open, no more data or edges will arrive
        wrote.send nil
      end
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
        wrote.receive
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

    it "can be closed while a fiber waits for data" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      done = Channel(Exception?).new
      spawn do
        reader.read_byte
        done.send nil
      rescue ex
        done.send ex
      end
      Fiber.yield
      reader.close
      select
      when ex = done.receive
        ex.should be_a IO::Error
      when timeout(2.seconds)
        fail "close didn't wake up the reader"
      end
    ensure
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
      pool.stats[:available].should eq 1
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

    it "releases the read buffer at end of stream" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      writer.write Bytes.new(10, 1_u8)
      writer.close
      buf = Bytes.new(16) # small reads go via the read buffer
      reader.read(buf).should eq 10
      reader.read(buf).should eq 0
      reader.@in_buffer.null?.should be_true
      pool.stats[:available].should eq 1
    ensure
      reader.try &.close
    end

    it "doesn't hold an emptied read buffer while reading directly into a large slice" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      writer.write_byte 1_u8
      reader.read_byte.should eq 1_u8 # empties, but keeps, the read buffer
      read = Channel(Int32).new
      # at least half the buffer size is read directly into the slice
      spawn { read.send reader.read(Bytes.new(600)) }
      Fiber.yield
      reader.@in_buffer.null?.should be_true
      writer.write Bytes.new(600)
      read.receive.should eq 600
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "doesn't change IOs without a buffer pool" do
      reader, writer = IO.pipe
      writer.puts "hello"
      writer.flush
      reader.gets.should eq "hello"
      reader.buffer_pool.should be_nil
    ensure
      reader.try &.close
      writer.try &.close
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
      pool.stats[:available].should eq 1
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

describe Socket do
  describe "#read_nonblock" do
    it "returns nil when no data is available" do
      reader, writer = UNIXSocket.pair
      reader.read_nonblock(Bytes.new(16)).should be_nil
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "returns the available data without waiting for more" do
      reader, writer = UNIXSocket.pair
      writer.write "hello".to_slice
      buf = Bytes.new(16)
      reader.read_nonblock(buf).should eq 5
      String.new(buf[0, 5]).should eq "hello"
      reader.read_nonblock(buf).should be_nil
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "returns 0 at end of stream" do
      reader, writer = UNIXSocket.pair
      writer.close
      reader.read_nonblock(Bytes.new(16)).should eq 0
    ensure
      reader.try &.close
    end

    it "raises when the socket is closed" do
      reader, writer = UNIXSocket.pair
      reader.close
      expect_raises(IO::Error) { reader.read_nonblock(Bytes.new(16)) }
    ensure
      writer.try &.close
    end
  end

  describe "#wait_readable" do
    it "returns when data arrives" do
      reader, writer = UNIXSocket.pair
      spawn { writer.write_byte 1_u8 }
      reader.wait_readable
      reader.read_nonblock(Bytes.new(16)).should eq 1
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "raises IO::TimeoutError after read_timeout" do
      reader, writer = UNIXSocket.pair
      reader.read_timeout = 10.milliseconds
      expect_raises(IO::TimeoutError) { reader.wait_readable }
    ensure
      reader.try &.close
      writer.try &.close
    end
  end
end
