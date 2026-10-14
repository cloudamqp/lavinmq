module LavinMQ
  # Stat log buffers in mmap'd slabs outside the GC heap. Expired leases replace finalizers,
  # and a new generation per handout lets readers detect reuse.
  class StatBlockPool
    SLAB_SIZE   = 1 << 20
    PAGE_SIZE   = LibC.sysconf(LibC::SC_PAGESIZE).to_i32
    HEADER      = 4 # UInt32 words: lease, generation, ordinal, capacity
    TRIM_TIME   = 2.milliseconds
    LEASE       = 64_u32 # ticks a block stays used without its owner renewing it
    SWEEP_TICKS =     32 # ticks to look at the leases of all used blocks in

    @@pools = Hash(Int32, StatBlockPool).new
    @@pools_lock = Mutex.new
    @@last : StatBlockPool? = nil
    @@tick = 0_u32

    def self.for(capacity : Int32) : StatBlockPool
      last = @@last
      return last if last && last.capacity == capacity
      @@last = @@pools_lock.synchronize { @@pools[capacity] ||= new(capacity) }
    end

    # Stats ticks so far, advanced by `trim`
    def self.tick : UInt32
      @@tick
    end

    # Trims every pool, once per stats tick, for about TRIM_TIME in all.
    def self.trim : Nil
      tick = @@tick &+= 1
      pools = @@pools_lock.synchronize { @@pools.values }
      deadline = Time.instant + TRIM_TIME
      pools.each &.trim(deadline, tick)
    end

    def self.generation(data : Pointer(UInt32)) : UInt32
      data[-3]
    end

    def self.ordinal(data : Pointer(UInt32)) : Int32
      data[-2].to_i32
    end

    def self.renew(data : Pointer(UInt32), tick : UInt32) : Nil
      data[-4] = tick
    end

    getter capacity : Int32
    @stride : Int32
    @slab_blocks : Int32
    @slab_bytes : Int32
    @slabs = Array(Slab).new
    @first_free = 0 # no free block in the slabs below
    @used = 0
    # Highest used ordinal + 1, as of the last trim
    getter span = 0
    @watermark = Int32::MAX
    @room = 0 # free blocks below the watermark not yet promised to a move
    @generation = 0_u32
    @next_sweep = 0 # where the next trim looks at leases from
    @next_slab = 0  # where the next trim returns pages from
    @lock = Mutex.new

    def initialize(@capacity : Int32)
      raise ArgumentError.new("capacity must be positive") unless @capacity.positive?
      @stride = ((@capacity + HEADER) * sizeof(UInt32) + 7) & ~7
      @slab_blocks = Math.max(SLAB_SIZE // @stride, 1)
      @slab_bytes = (@slab_blocks * @stride + PAGE_SIZE - 1) // PAGE_SIZE * PAGE_SIZE
    end

    def alloc : Pointer(UInt32)
      @lock.synchronize do
        loop do
          @slabs << Slab.new(@slab_blocks, @slab_bytes, @stride) if @first_free == @slabs.size
          if index = @slabs[@first_free].take
            @used += 1
            return data(@first_free * @slab_blocks + index)
          end
          @first_free += 1
        end
      end
    end

    def release(data : Pointer(UInt32)) : Nil
      @lock.synchronize { free(StatBlockPool.ordinal(data)) }
    end

    # Frees expired blocks and returns free pages to the OS until *deadline*
    def trim(deadline : Time::Instant? = nil, tick : UInt32 = StatBlockPool.tick) : Nil
      @lock.synchronize do
        expire(tick)
        return_pages(deadline)
        plan_moves
      end
    end

    # Voids the generation first, so views taken before read their constant from here on
    private def free(ordinal : Int32) : Nil
      slab_index, index = ordinal.divmod(@slab_blocks)
      slab = @slabs[slab_index]
      slab.block(index).as(Pointer(UInt32))[1] = 0_u32
      slab.free(index)
      @used -= 1
      @first_free = Math.min(@first_free, slab_index)
    end

    private def expire(tick : UInt32) : Nil
      ((@slabs.size + SWEEP_TICKS - 1) // SWEEP_TICKS).times do
        @next_sweep = 0 if @next_sweep >= @slabs.size
        slab_index = @next_sweep
        @next_sweep += 1
        slab = @slabs[slab_index]
        slab.each_used do |index|
          lease = slab.block(index).as(Pointer(UInt32)).value
          free(slab_index * @slab_blocks + index) if tick &- lease > LEASE
        end
      end
    end

    private def return_pages(deadline : Time::Instant?) : Nil
      @slabs.size.times do
        @next_slab = 0 if @next_slab >= @slabs.size
        slab = @slabs[@next_slab]
        @next_slab += 1
        next unless slab.dirty?
        slab.return_pages
        return if deadline && Time.instant >= deadline
      end
    end

    # Whether blocks above the watermark should move down, if there's room below it
    def relocate?(high : Int32, count : Int32) : Bool
      return false if high < @watermark
      @lock.synchronize do
        return false if @room < count
        @room -= count
        true
      end
    end

    private def plan_moves : Nil
      last = @slabs.rindex { |slab| slab.used > 0 }
      @span = last ? last * @slab_blocks + @slabs[last].highest_used + 1 : 0
      if @span > 2 * @used
        @watermark = @used + @used // 4
        whole, rest = @watermark.divmod(@slab_blocks)
        @room = (0...whole).sum { |i| @slab_blocks - @slabs[i].used } + @slabs[whole].free_below(rest)
      else
        @watermark = Int32::MAX
        @room = 0
      end
    end

    # Headed by a new generation, published before the owner fills the block
    private def data(ordinal : Int32) : Pointer(UInt32)
      slab_index, index = ordinal.divmod(@slab_blocks)
      block = @slabs[slab_index].block(index).as(Pointer(UInt32))
      block[0] = StatBlockPool.tick
      block[1] = @generation &+= 1
      block[2] = ordinal.to_u32
      block[3] = @capacity.to_u32
      Atomic.fence(:release)
      block + HEADER
    end

    private class Slab
      @base : Pointer(UInt8)
      @free : Slice(UInt64)     # a set bit per free block
      @returned : Slice(UInt64) # a set bit per page not backed by memory
      @dirty = false            # blocks freed since return_pages
      getter used = 0

      def initialize(@blocks : Int32, @bytes : Int32, @stride : Int32)
        ptr = LibC.mmap(nil, @bytes, LibC::PROT_READ | LibC::PROT_WRITE, LibC::MAP_PRIVATE | LibC::MAP_ANON, -1, 0)
        raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
        @base = ptr.as(Pointer(UInt8))
        @free = bitmap(@blocks)
        @returned = bitmap(@bytes // PAGE_SIZE)
      end

      def block(index : Int32) : Pointer(UInt8)
        @base + index.to_i64 * @stride
      end

      def take : Int32?
        @free.each_with_index do |word, w|
          next if word.zero?
          bit = word.trailing_zeros_count
          @free[w] = word & ~(1_u64 << bit)
          @used += 1
          index = w * 64 + bit
          each_page(index) { |page| clear(@returned, page) }
          return index
        end
      end

      def free(index : Int32) : Nil
        set(@free, index)
        @used -= 1
        @dirty = true
      end

      def each_used(& : Int32 ->) : Nil
        @free.size.times do |w|
          used = ~@free[w] & valid_bits(w)
          until used.zero?
            yield w * 64 + used.trailing_zeros_count
            used &= used - 1
          end
        end
      end

      def highest_used : Int32
        (@free.size - 1).downto(0) do |w|
          used = ~@free[w] & valid_bits(w)
          return w * 64 + 63 - used.leading_zeros_count unless used.zero?
        end
        -1
      end

      # Free blocks among the first *count*
      def free_below(count : Int32) : Int32
        whole, rest = count.divmod(64)
        free = (0...whole).sum { |w| @free[w].popcount }
        free += (@free[whole] & ((1_u64 << rest) - 1)).popcount if rest > 0
        free.to_i32
      end

      private def valid_bits(word : Int32) : UInt64
        rest = @blocks - word * 64
        rest >= 64 ? UInt64::MAX : (1_u64 << rest) - 1
      end

      def dirty? : Bool
        @dirty
      end

      def return_pages : Nil
        @dirty = false
        run = 0
        pages = @bytes // PAGE_SIZE
        pages.times do |page|
          if !set?(@returned, page) && page_free?(page)
            set(@returned, page)
            run += 1
          else
            dontneed(page - run, run)
            run = 0
          end
        end
        dontneed(pages - run, run)
      end

      private def dontneed(page : Int32, count : Int32) : Nil
        return if count.zero?
        if LibC.madvise(@base + page.to_i64 * PAGE_SIZE, count.to_i64 * PAGE_SIZE, LibC::MADV_DONTNEED) != 0
          raise RuntimeError.from_errno("madvise")
        end
      end

      private def page_free?(page : Int32) : Bool
        first = page.to_i64 * PAGE_SIZE // @stride
        last = Math.min(((page + 1).to_i64 * PAGE_SIZE - 1) // @stride, @blocks - 1)
        (first..last).all? { |i| set?(@free, i.to_i32) }
      end

      private def each_page(index : Int32, &)
        start = index.to_i64 * @stride
        (start // PAGE_SIZE).upto((start + @stride - 1) // PAGE_SIZE) { |page| yield page.to_i32 }
      end

      private def bitmap(bits : Int32) : Slice(UInt64)
        map = Slice(UInt64).new((bits + 63) // 64, UInt64::MAX)
        map[-1] = UInt64::MAX >> (map.size * 64 - bits) if bits % 64 != 0
        map
      end

      private def set?(map : Slice(UInt64), i : Int32) : Bool
        map[i // 64].bit(i % 64) == 1
      end

      private def set(map : Slice(UInt64), i : Int32) : Nil
        map[i // 64] |= 1_u64 << (i % 64)
      end

      private def clear(map : Slice(UInt64), i : Int32) : Nil
        map[i // 64] &= ~(1_u64 << (i % 64))
      end
    end
  end
end
