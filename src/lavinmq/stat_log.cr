require "json"
require "../stdlib/json_builder"
require "./stat_block_pool"

module LavinMQ
  # A stat's last value, plus a pooled ring buffer while it changes
  struct StatColumn
    getter last : UInt32
    # Inverted so the conservative GC doesn't scan it as a pointer
    @hidden : UInt64 = UInt64::MAX

    def initialize(@last : UInt32)
    end

    @[AlwaysInline]
    def block : Pointer(UInt32)
      Pointer(UInt32).new(~@hidden)
    end

    @[AlwaysInline]
    def buffered? : Bool
      @hidden != UInt64::MAX
    end

    # Returns whether the value changed; takes a buffer on the first change
    @[AlwaysInline]
    def record(value : UInt32, tail : Int32, established : Bool, & : UInt32 -> Pointer(UInt32)) : Bool
      buf = block
      if value == @last
        buf[tail] = value if buf
        return false
      end
      if buf
        buf[tail] = value
      elsif established
        buf = yield @last
        buf[tail] = value
        self.block = buf
      end
      @last = value
      true
    end

    def renew(tick : UInt32) : Nil
      if buf = block
        StatBlockPool.renew(buf, tick)
      end
    end

    # Fills the rest with the last value, as readers may pair *buf* with the old head and size
    def move(buf : Pointer(UInt32), old_cap : Int32, head : Int32, size : Int32, new_cap : Int32) : Pointer(UInt32)
      old = block
      keep = Math.min(size, new_cap)
      first = head + size - keep
      keep.times do |k|
        src = first + k
        src -= old_cap if src >= old_cap
        buf[k] = old[src]
      end
      Slice.new(buf + keep, new_cap - keep).fill(@last)
      Atomic.fence(:release)
      self.block = buf
      old
    end

    def drop : Pointer(UInt32)
      old = block
      self.block = Pointer(UInt32).null
      old
    end

    private def block=(buf : Pointer(UInt32)) : Nil
      @hidden = ~buf.address
    end
  end

  # Reads `const` once the buffer is handed out again, and never past its header's capacity
  struct StatLogView
    include Enumerable(UInt32)

    @generation = 0_u32
    @capacity = 0

    def initialize(@base : Pointer(UInt32), @head : Int32, @size : Int32, @const : UInt32)
      if @base
        @generation = StatBlockPool.generation(@base)
        @capacity = @base[-1].to_i
        # Zeroed header: the page was given back to the OS
        @base = Pointer(UInt32).null if @capacity.zero?
      end
    end

    getter const : UInt32

    def constant? : Bool
      @base.null?
    end

    def size : Int32
      @base.null? ? @size : Math.min(@size, @capacity)
    end

    def [](index : Int32) : UInt32
      raise IndexError.new if index < 0 || index >= size
      return @const if @base.null?
      value = @base[wrap(@head % @capacity + index, @capacity)]
      reused? ? @const : value
    end

    CHUNK = 64

    # Checks the generation once per chunk
    def each(& : UInt32 ->) : Nil
      n = size
      if @base.null?
        n.times { yield @const }
        return
      end
      chunk = uninitialized UInt32[CHUNK]
      i = @head % @capacity
      done = 0
      while done < n
        count = Math.min(n - done, CHUNK)
        count.times do |k|
          chunk[k] = @base[i]
          i = wrap(i + 1, @capacity)
        end
        if reused?
          (n - done).times { yield @const }
          return
        end
        count.times { |k| yield chunk[k] }
        done += count
      end
    end

    @@const_json : ConstJson(UInt32)? = nil

    def to_json(json : JSON::Builder) : Nil
      if @base.null?
        cached = @@const_json
        cached = @@const_json = ConstJson.new(@const, @size) unless cached && cached.for?(@const, @size)
        json.raw(cached.json)
      else
        StatJson.array(json, self, tenths: false)
      end
    end

    # Checked after the read, as a new owner writes the generation before samples
    @[AlwaysInline]
    private def reused? : Bool
      Atomic.fence(:acquire)
      StatBlockPool.generation(@base) != @generation
    end

    private def wrap(i : Int32, cap : Int32) : Int32
      i >= cap ? i - cap : i
    end
  end

  # Rates stored as tenths in UInt32 samples
  struct RateLogView
    include Enumerable(Float64)

    def self.encode(rate : Float64) : UInt32
      (rate * 10).round.clamp(0.0, UInt32::MAX.to_f64).to_u32
    end

    # Same as `encode((delta / interval).round(1))`, with one rounding
    def self.encode_delta(delta : UInt64, interval : Float64) : UInt32
      (delta / interval * 10).round.clamp(0.0, UInt32::MAX.to_f64).to_u32
    end

    def self.decode(tenths : UInt32) : Float64
      tenths / 10.0
    end

    def initialize(@tenths : StatLogView)
    end

    delegate size, to: @tenths

    def [](index : Int32) : Float64
      RateLogView.decode(@tenths[index])
    end

    def each(& : Float64 ->) : Nil
      @tenths.each { |t| yield RateLogView.decode(t) }
    end

    @@const_json : ConstJson(Float64)? = nil

    def to_json(json : JSON::Builder) : Nil
      if @tenths.constant?
        rate = RateLogView.decode(@tenths.const)
        cached = @@const_json
        cached = @@const_json = ConstJson.new(rate, size) unless cached && cached.for?(rate, size)
        json.raw(cached.json)
      else
        StatJson.array(json, @tenths, tenths: true)
      end
    end
  end

  # Formats a log as JSON in a stack buffer; tenths print as the Float64 they decode to
  module StatJson
    BUFFER = 1024

    def self.array(json : JSON::Builder, view : StatLogView, tenths : Bool) : Nil
      json.raw do |io|
        buf = uninitialized UInt8[BUFFER]
        ptr = buf.to_unsafe
        ptr[0] = '['.ord.to_u8
        n = 1
        first = true
        view.each do |value|
          if n > BUFFER - 14
            io.write_string(Slice.new(ptr, n))
            n = 0
          end
          unless first
            ptr[n] = ','.ord.to_u8
            n += 1
          end
          first = false
          n += tenths ? write_tenths(ptr + n, value) : write_digits(ptr + n, value)
        end
        ptr[n] = ']'.ord.to_u8
        io.write_string(Slice.new(ptr, n + 1))
      end
    end

    private def self.write_tenths(ptr : Pointer(UInt8), tenths : UInt32) : Int32
      len = write_digits(ptr, tenths // 10)
      ptr[len] = '.'.ord.to_u8
      ptr[len + 1] = 48_u8 &+ (tenths % 10).to_u8!
      len + 2
    end

    private def self.write_digits(ptr : Pointer(UInt8), value : UInt32) : Int32
      digits = uninitialized UInt8[10]
      i = 10
      loop do
        i -= 1
        digits[i] = 48_u8 &+ (value % 10).to_u8!
        value //= 10
        break if value.zero?
      end
      ptr.copy_from(digits.to_unsafe + i, 10 - i)
      10 - i
    end
  end

  # Idle logs are mostly the same few arrays, so the last one built is cached
  class ConstJson(T)
    getter json : String

    def initialize(@value : T, @size : Int32)
      text = @value.to_json
      @json = @size > 0 ? "[#{text}#{",#{text}" * (@size - 1)}]" : "[]"
    end

    def for?(value : T, size : Int32) : Bool
      @value == value && @size == size
    end
  end
end
