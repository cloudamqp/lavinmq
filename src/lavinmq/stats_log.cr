module LavinMQ
  # Per tick history of every stats series (per queue, channel, connection
  # etc), stored column-wise in memory mapped outside the GC heap.
  #
  # `Stats.counter_log` holds how much each counter increased per tick, from
  # which the rates are computed when read, and `Stats.gauge_log` holds the
  # values of gauges. The stats loop advances both together, so owners keep a
  # single start tick for their series in both.
  #
  # Series are grouped in chunks of `chunk_slots`. A chunk is time-major:
  # `size` rows of `chunk_slots` values, one row per stats tick. Advancing the
  # tick zeroes the next row in every chunk, so a series only has to be
  # written when its value is non-zero.
  #
  # A series is given a slot on its first non-zero value and loses it after a
  # full window of zeros, when all its rows have been zeroed already, so a
  # free slot can be reused without clearing it. Idle series therefore use no
  # memory, and slots of deleted queues, closed channels etc are reclaimed
  # without their owners releasing them. A chunk without used slots is
  # unmapped, so the memory is returned to the OS.
  #
  # Owners keep a `Series` handle. Every allocation gets a new id, so a handle
  # to a reclaimed slot is detected and reads as zeros.
  class StatsLog(T)
    record Series, slot : Int32 = -1, id : UInt64 = 0u64

    getter size : Int32
    getter chunk_slots : Int32
    getter tick = 0i64
    @chunks = Array(Chunk(T)?).new
    @free_hint = 0 # no free slot below this one
    @last_id = 0u64
    @lock = Mutex.new(:unchecked)

    def initialize(@size : Int32, @chunk_slots : Int32 = 1024)
      raise ArgumentError.new("size must be positive") unless @size.positive?
      raise ArgumentError.new("chunk_slots must be positive") unless @chunk_slots.positive?
    end

    # Number of ticks since *start_tick*, capped by the window size
    def ticks_since(start_tick : Int64) : Int32
      (@tick - start_tick).clamp(0i64, @size.to_i64).to_i32
    end

    # Number of slots in use
    def series_count : Int32
      @lock.synchronize { @chunks.sum { |c| c.try(&.used) || 0 } }
    end

    # Number of mapped chunks
    def chunk_count : Int32
      @lock.synchronize { @chunks.count(&.itself) }
    end

    # Starts a new tick: the oldest row is zeroed and becomes the current one,
    # and slots that have been zero for a full window are freed.
    def advance : Nil
      @lock.synchronize do
        @tick += 1
        row = (@tick % @size).to_i32
        @chunks.each_with_index do |chunk, idx|
          next unless chunk
          chunk.clear_row(row)
          chunk.free_idle(@tick - @size) do |offset|
            @free_hint = Math.min(@free_hint, idx * @chunk_slots + offset)
          end
          if chunk.used.zero?
            chunk.unmap
            @chunks[idx] = nil
          end
        end
        while @chunks.size.positive? && @chunks.last.nil?
          @chunks.pop
        end
      end
    end

    # Changes the number of ticks kept, keeping the latest values
    def resize(size : Int32) : Nil
      raise ArgumentError.new("size must be positive") unless size.positive?
      @lock.synchronize do
        return if size == @size
        @chunks.each do |chunk|
          chunk.try &.resize(@size, size, @tick)
        end
        @size = size
      end
    end

    # Stores *value* for *series* at the current tick. Returns the handle to
    # keep, which differs from *series* if a slot had to be (re)allocated.
    # Zeros don't need a slot, as rows are zeroed when the tick advances.
    def write(series : Series, value : T) : Series
      @lock.synchronize do
        chunk = chunk_for(series)
        unless chunk
          return series if value.zero?
          series, chunk = allocate
        end
        chunk.write((@tick % @size).to_i32, series.slot % @chunk_slots, value, @tick)
        series
      end
    end

    # The sum of *series* for each of the last *count* ticks, oldest first,
    # as converted by the block
    def read(count : Int32, *series : Series, & : Int64 -> U) : Array(U) forall U
      log = Array(U).new(count.clamp(0, @size), U.zero)
      merge_into(log, count, *series) { |_, sum| yield sum }
      log
    end

    # Replaces each value in *sums* with the block's result for it and the sum
    # of *series* at the same tick, aligned at the latest tick. If *sums* has
    # fewer than *count* values it's first padded with zeros at the front.
    def merge_into(sums : Array(U), count : Int32, *series : Series, & : U, Int64 -> U) : Nil forall U
      count = count.clamp(0, @size)
      until sums.size >= count
        sums.unshift U.zero
      end
      offset = sums.size - count
      @lock.synchronize do
        chunks = series.map { |s| chunk_for(s) }
        count.times do |i|
          row = ((@tick - count + 1 + i) % @size).to_i32
          sum = 0i64
          series.each_with_index do |s, j|
            if chunk = chunks[j]
              sum += chunk.read(row, s.slot % @chunk_slots)
            end
          end
          sums[offset + i] = yield sums[offset + i], sum
        end
      end
    end

    # The chunk holding *series*, unless its slot has been reclaimed
    private def chunk_for(series : Series) : Chunk(T)?
      return if series.slot.negative?
      chunk = @chunks[series.slot // @chunk_slots]?
      chunk if chunk && chunk.id(series.slot % @chunk_slots) == series.id
    end

    # Takes the lowest free slot, so that the highest chunks drain and can be
    # unmapped when series are reclaimed.
    private def allocate : {Series, Chunk(T)}
      slot = @free_hint
      loop do
        idx = slot // @chunk_slots
        until idx < @chunks.size
          @chunks << nil
        end
        chunk = @chunks[idx]
        if chunk.nil? || chunk.free?(slot % @chunk_slots)
          chunk ||= @chunks[idx] = Chunk(T).new(@size, @chunk_slots)
          id = @last_id += 1
          chunk.take(slot % @chunk_slots, id, @tick)
          @free_hint = slot + 1
          return {Series.new(slot, id), chunk}
        end
        slot += 1
      end
    end

    private class Chunk(V)
      getter used = 0
      # id of the series owning each slot, 0 when free
      @ids : Slice(UInt64)
      # tick of the latest non-zero value of each slot
      @last_write : Slice(Int64)
      @values : Pointer(V)
      @bytesize : LibC::SizeT

      def initialize(rows : Int32, @slots : Int32)
        @ids = Slice(UInt64).new(@slots)
        @last_write = Slice(Int64).new(@slots)
        @bytesize = bytesize(rows)
        @values = map(@bytesize)
      end

      private def bytesize(rows) : LibC::SizeT
        LibC::SizeT.new(rows) * @slots * sizeof(V)
      end

      private def map(bytesize) : Pointer(V)
        ptr = LibC.mmap(nil, bytesize, LibC::PROT_READ | LibC::PROT_WRITE,
          LibC::MAP_PRIVATE | LibC::MAP_ANON, -1, 0)
        raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
        ptr.as(Pointer(V))
      end

      def unmap : Nil
        return if @values.null?
        LibC.munmap(@values, @bytesize)
        @values = Pointer(V).null
      end

      def finalize
        unmap
      end

      # Moves the rows of the latest ticks, as many as fit, to a new mapping with *rows* rows
      def resize(old_rows : Int32, rows : Int32, tick : Int64) : Nil
        bytesize = bytesize(rows)
        values = map(bytesize)
        Math.min(old_rows, rows).times do |i|
          t = tick - i
          (values + (t % rows) * @slots).copy_from(@values + (t % old_rows) * @slots, @slots)
        end
        unmap
        @values = values
        @bytesize = bytesize
      end

      def id(offset) : UInt64
        @ids[offset]
      end

      def free?(offset) : Bool
        @ids[offset].zero?
      end

      def take(offset, id : UInt64, tick : Int64) : Nil
        @ids[offset] = id
        @last_write[offset] = tick
        @used += 1
      end

      # Frees the slots whose latest non-zero value is at or before *tick*
      def free_idle(tick : Int64, & : Int32 ->) : Nil
        @slots.times do |offset|
          next if @ids[offset].zero?
          next if @last_write[offset] > tick
          @ids[offset] = 0u64
          @used -= 1
          yield offset
        end
      end

      def clear_row(row : Int32) : Nil
        (@values + row * @slots).clear(@slots)
      end

      def write(row : Int32, offset : Int32, value : V, tick : Int64) : Nil
        @values[row * @slots + offset] = value
        @last_write[offset] = tick unless value.zero?
      end

      def read(row : Int32, offset : Int32) : V
        @values[row * @slots + offset]
      end
    end
  end
end
