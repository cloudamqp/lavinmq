require "spec"
require "../../src/stdlib/io_buffered"
require "../../src/stdlib/io_buffered_discard"

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

  it "keeps at most CACHE_BYTES_PER_THREAD of buffers per thread" do
    pool = IO::BufferPool.new(16384)
    pool.max_cached.should eq 256
    extra = 5
    buffers = Array.new(pool.max_cached + extra) { pool.acquire }
    buffers.each { |b| pool.release(b) }
    stats = pool.stats
    stats[:available].should eq pool.max_cached
    stats[:released].should eq pool.max_cached
    stats[:dropped].should eq extra
  end

  it "caches at least one buffer of a size above CACHE_BYTES_PER_THREAD" do
    IO::BufferPool.new(IO::BufferPool::CACHE_BYTES_PER_THREAD * 2).max_cached.should eq 1
  end

  it "caches buffers per thread" do
    pool = IO::BufferPool.new(1024)
    # Pinned to a thread of its own: a fiber of the default context can
    # resume on another of its threads after waiting
    Fiber::ExecutionContext::Isolated.new("acquire on one thread") do
      buf = pool.acquire
      Fiber::ExecutionContext::Isolated.new("release on another thread") do
        pool.release(buf)
      end.wait
      # released into the other thread's cache, not this one's
      pool.acquire.should_not eq buf
    end.wait
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
      spawn { read.send reader.read_byte.not_nil!("unexpected EOF") }
      Fiber.yield
      reader.@in_buffer.null?.should be_true
      writer.write_byte 7_u8
      writer.flush
      read.receive.should eq 7_u8
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "doesn't hold an emptied read buffer while waiting for more data" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = pool
      reader.read_buffering = true
      read = Channel(UInt8).new
      spawn { 2.times { read.send reader.read_byte.not_nil!("unexpected EOF") } }
      writer.write_byte 1_u8
      writer.flush
      read.receive.should eq 1_u8
      # read_byte keeps the emptied buffer, the next read_byte waits without it
      Fiber.yield
      reader.@in_buffer.null?.should be_true
      writer.write_byte 2_u8
      writer.flush
      read.receive.should eq 2_u8
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

    it "doesn't write a blocked flush's buffer again when closed concurrently" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      filled = fill_send_buffer(writer)
      done = Channel(Nil).new
      spawn do
        writer.write Bytes.new(512, 1_u8)
        writer.flush # blocks, the socket's buffers are full
      rescue IO::Error
      ensure
        done.send nil
      end
      # until the flush has detached the write buffer
      until writer.@out_buffer.null? && writer.@out_count.zero? && pool.stats[:allocated] == 1
        Fiber.yield
      end
      spawn { writer.close } # flushes concurrently without the client's write lock
      Fiber.yield
      received = reader.gets_to_end.bytesize
      done.receive
      # at most the blocked write, not its buffer a second time (which may by
      # then be back in the pool and filled by another connection)
      received.should be <= filled + 512
      acquired = pool.stats[:allocated] + pool.stats[:reused]
      pool.stats[:released].should eq acquired
    ensure
      reader.try &.close
    end

    it "returns the write buffer to the pool when closed with buffered data" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      writer.write Bytes.new(10, 1_u8)
      writer.close
      reader.gets_to_end.to_slice.should eq Bytes.new(10, 1_u8)
      pool.stats[:available].should eq 1
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

    it "doesn't allow changing buffer_size while using a pool" do
      reader, writer = UNIXSocket.pair
      reader.buffer_pool = IO::BufferPool.new(1024)
      reader.buffer_size = 1024
      expect_raises(ArgumentError) { reader.buffer_size = 4096 }
      reader.buffer_size.should eq 1024
    ensure
      reader.try &.close
      writer.try &.close
    end

    it "releases the read buffer when a socket read fails" do
      pool = IO::BufferPool.new(1024)
      server = TCPServer.new("127.0.0.1", 0)
      client = TCPSocket.new("127.0.0.1", server.local_address.port)
      peer = server.accept
      client.buffer_pool = pool
      client.read_buffering = true
      peer.linger = 0
      peer.close # resets the connection
      expect_raises(IO::Error) { client.read_byte }
      pool.stats[:available].should eq 1
    ensure
      client.try &.close
      server.try &.close
    end

    it "releases the read buffer when an unbuffered read fails" do
      pool = IO::BufferPool.new(1024)
      io = ScriptedIO.new { raise IO::Error.new("read failed") }
      io.buffer_pool = pool
      expect_raises(IO::Error) { io.read_byte }
      pool.stats[:available].should eq 1
    end

    it "releases the read buffer when an unbuffered read fails after a close" do
      pool = IO::BufferPool.new(1024)
      gate = Channel(Nil).new
      io = ScriptedIO.new do
        gate.receive? # waits for data, like SSL_read on a quiet connection
        raise IO::Error.new("Closed stream")
      end
      io.on_close = -> { gate.close }
      io.buffer_pool = pool
      done = Channel(Nil).new
      spawn do
        io.read_byte rescue nil
        done.send nil
      end
      until pool.stats[:allocated] == 1 # the reader holds the read buffer
        Fiber.yield
      end
      io.close
      done.receive
      pool.stats[:available].should eq 1
    end

    it "releases the read buffer at end of stream of a non-socket IO" do
      pool = IO::BufferPool.new(1024)
      io = ScriptedIO.new { 0 }
      io.buffer_pool = pool
      io.read_byte.should be_nil
      pool.stats[:available].should eq 1
    end

    it "releases the write buffer when a flush fails" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      reader.close
      writer.write Bytes.new(10)
      expect_raises(IO::Error) { writer.flush }
      writer.@out_buffer.null?.should be_true
      pool.stats[:available].should eq 1
    ensure
      writer.try &.close
    end

    it "releases the write buffer when it's discarded" do
      pool = IO::BufferPool.new(1024)
      reader, writer = UNIXSocket.pair
      writer.buffer_pool = pool
      writer.sync = false
      writer.write Bytes.new(10)
      writer.discard_write_buffer
      pool.stats[:available].should eq 1
      writer.close
      reader.read_timeout = 1.second
      reader.gets_to_end.should be_empty
    ensure
      reader.try &.close
    end
  end
end

# An IO::Buffered whose reads run a block, for the read path that sockets
# don't take (TLS sockets do)
class ScriptedIO < IO
  include IO::Buffered

  property on_close : Proc(Nil)? = nil

  def initialize(&@on_read : -> Int32)
  end

  def unbuffered_read(slice : Bytes)
    @on_read.call
  end

  def unbuffered_write(slice : Bytes) : Nil
  end

  def unbuffered_flush
  end

  def unbuffered_close
    @on_close.try &.call
  end

  def unbuffered_rewind
  end
end

# Fills the socket's send buffer, so that its next write blocks until the
# peer reads. Returns the number of bytes written.
def fill_send_buffer(socket : Socket) : Int64
  total = 0_i64
  {4096, 512, 1}.each do |size|
    buf = Bytes.new(size)
    while (n = LibC.send(socket.fd, buf, buf.size, LibC::MSG_DONTWAIT)) > 0
      total += n
    end
  end
  total
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
      expect_raises(IO::TimeoutError, "Read timed out") { reader.wait_readable }
    ensure
      reader.try &.close
      writer.try &.close
    end
  end
end
