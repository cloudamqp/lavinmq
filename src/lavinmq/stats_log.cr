require "./config"

module LavinMQ
  # Rate history for every stats series (per queue, channel, connection etc),
  # stored column-wise in memory mapped outside the GC heap.
  #
  # Series are grouped in chunks of `CHUNK_SLOTS`. A chunk is time-major:
  # `size` rows of `CHUNK_SLOTS` values, one row per stats tick. Advancing the
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
  class StatsLog
    CHUNK_SLOTS = 1024

    record Series, slot : Int32 = -1, id : UInt64 = 0u64

    class_property(instance : StatsLog) { new(Config.instance.stats_log_size) }

    getter size : Int32
    getter tick = 0i64
    @chunks = Array(Chunk?).new
    @free_hint = 0 # no free slot below this one
    @last_id = 0u64
    @lock = Mutex.new(:unchecked)

    def initialize(@size : Int32)
      raise ArgumentError.new("size must be positive") unless @size.positive?
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
            @free_hint = Math.min(@free_hint, idx * CHUNK_SLOTS + offset)
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
    def write(series : Series, value : Float64) : Series
      @lock.synchronize do
        chunk = chunk_for(series)
        series, chunk = allocate unless chunk
        chunk.write((@tick % @size).to_i32, series.slot % CHUNK_SLOTS, value.to_f32, @tick)
        series
      end
    end

    # The sum of *series* for each of the last *count* ticks, oldest first,
    # rounded to one decimal.
    def read(count : Int32, *series : Series) : Array(Float64)
      count = count.clamp(0, @size)
      log = Array(Float64).new(count, 0.0)
      @lock.synchronize do
        series.each do |s|
          next unless chunk = chunk_for(s)
          offset = s.slot % CHUNK_SLOTS
          count.times do |i|
            row = ((@tick - count + 1 + i) % @size).to_i32
            log[i] += chunk.read(row, offset)
          end
        end
      end
      log.map! &.round(1)
    end

    # The chunk holding *series*, unless its slot has been reclaimed
    private def chunk_for(series : Series) : Chunk?
      return if series.slot.negative?
      chunk = @chunks[series.slot // CHUNK_SLOTS]?
      chunk if chunk && chunk.id(series.slot % CHUNK_SLOTS) == series.id
    end

    # Takes the lowest free slot, so that the highest chunks drain and can be
    # unmapped when series are reclaimed.
    private def allocate : {Series, Chunk}
      slot = @free_hint
      loop do
        idx = slot // CHUNK_SLOTS
        until idx < @chunks.size
          @chunks << nil
        end
        chunk = @chunks[idx]
        if chunk.nil? || chunk.free?(slot % CHUNK_SLOTS)
          chunk ||= @chunks[idx] = Chunk.new(@size)
          id = @last_id += 1
          chunk.take(slot % CHUNK_SLOTS, id, @tick)
          @free_hint = slot + 1
          return {Series.new(slot, id), chunk}
        end
        slot += 1
      end
    end

    private class Chunk
      getter used = 0
      # id of the series owning each slot, 0 when free
      @ids = Slice(UInt64).new(CHUNK_SLOTS)
      # tick of the latest non-zero value of each slot
      @last_write = Slice(Int64).new(CHUNK_SLOTS)
      @values : Pointer(Float32)
      @bytesize : LibC::SizeT

      def initialize(rows : Int32)
        @bytesize = self.class.bytesize(rows)
        @values = self.class.map(@bytesize)
      end

      protected def self.bytesize(rows) : LibC::SizeT
        LibC::SizeT.new(rows) * CHUNK_SLOTS * sizeof(Float32)
      end

      protected def self.map(bytesize) : Pointer(Float32)
        ptr = LibC.mmap(nil, bytesize, LibC::PROT_READ | LibC::PROT_WRITE,
          LibC::MAP_PRIVATE | LibC::MAP_ANON, -1, 0)
        raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
        ptr.as(Pointer(Float32))
      end

      # Moves the rows of the latest ticks, as many as fit, to a new mapping with *rows* rows
      def resize(old_rows : Int32, rows : Int32, tick : Int64) : Nil
        bytesize = self.class.bytesize(rows)
        values = self.class.map(bytesize)
        Math.min(old_rows, rows).times do |i|
          t = tick - i
          (values + (t % rows) * CHUNK_SLOTS).copy_from(@values + (t % old_rows) * CHUNK_SLOTS, CHUNK_SLOTS)
        end
        unmap
        @values = values
        @bytesize = bytesize
      end

      def unmap : Nil
        return if @values.null?
        LibC.munmap(@values, @bytesize)
        @values = Pointer(Float32).null
      end

      def finalize
        unmap
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

      # Frees the slots whose latest write is at or before *tick*
      def free_idle(tick : Int64, & : Int32 ->) : Nil
        CHUNK_SLOTS.times do |offset|
          next if @ids[offset].zero?
          next if @last_write[offset] > tick
          @ids[offset] = 0u64
          @used -= 1
          yield offset
        end
      end

      def clear_row(row : Int32) : Nil
        (@values + row * CHUNK_SLOTS).clear(CHUNK_SLOTS)
      end

      def write(row : Int32, offset : Int32, value : Float32, tick : Int64) : Nil
        @values[row * CHUNK_SLOTS + offset] = value
        @last_write[offset] = tick
      end

      def read(row : Int32, offset : Int32) : Float32
        @values[row * CHUNK_SLOTS + offset]
      end
    end
  end
end
