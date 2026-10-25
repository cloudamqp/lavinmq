require "json"
require "../stdlib/json_builder"
require "./stat_block_pool"

module LavinMQ
  # A stat's last value, plus a pooled ring buffer of T samples while it changes
  struct StatColumn(T)
    getter last : T
    # Inverted so the conservative GC doesn't scan it as a pointer
    @hidden : UInt64 = UInt64::MAX

    def initialize(@last : T)
    end

    @[AlwaysInline]
    def block : Pointer(T)
      Pointer(T).new(~@hidden)
    end

    @[AlwaysInline]
    def buffered? : Bool
      @hidden != UInt64::MAX
    end

    # Returns whether the value changed; takes a buffer on the first change
    @[AlwaysInline]
    def record(value : T, tail : Int32, established : Bool, & : T -> Pointer(T)) : Bool
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

    # Same capacity: laid out as before, so readers pair *buf* with the unchanged head and size.
    # New capacity: compacted to the front and filled up with the last value, as a reader may
    # still pair *buf* with the old head and size
    def move(buf : Pointer(T), old_cap : Int32, head : Int32, size : Int32, new_cap : Int32) : Pointer(T)
      old = block
      if new_cap == old_cap
        buf.copy_from(old, old_cap)
      else
        keep = Math.min(size, new_cap)
        first = head + size - keep
        keep.times do |k|
          src = first + k
          src -= old_cap if src >= old_cap
          buf[k] = old[src]
        end
        Slice.new(buf + keep, new_cap - keep).fill(@last)
      end
      Atomic.fence(:release)
      self.block = buf
      old
    end

    def drop : Pointer(T)
      old = block
      self.block = Pointer(T).null
      old
    end

    private def block=(buf : Pointer(T)) : Nil
      @hidden = ~buf.address
    end
  end

  # Reads `const` once the buffer is handed out again, and never past its header's capacity
  struct StatLogView(T)
    include Enumerable(T)

    @generation = 0_u32
    @capacity = 0

    def initialize(@base : Pointer(T), @head : Int32, @size : Int32, @const : T)
      if @base
        @generation = StatBlockPool.generation(@base)
        @capacity = StatBlockPool.bytes(@base) // sizeof(T)
        # A zeroed generation or capacity: the block was released, or its page given back to the OS
        @base = Pointer(T).null if @generation.zero? || @capacity.zero?
      end
    end

    getter const : T

    def constant? : Bool
      @base.null?
    end

    def size : Int32
      @base.null? ? @size : Math.min(@size, @capacity)
    end

    def [](index : Int32) : T
      raise IndexError.new if index < 0 || index >= size
      return @const if @base.null?
      value = @base[wrap(@head % @capacity + index, @capacity)]
      reused? ? @const : value
    end

    CHUNK = 64

    # Checks the generation once per chunk
    def each(& : T ->) : Nil
      n = size
      if @base.null?
        n.times { yield @const }
        return
      end
      chunk = uninitialized T[CHUNK]
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

    @@const_json : ConstJson(UInt64)? = nil

    def to_json(json : JSON::Builder) : Nil
      if @base.null?
        const = @const.to_u64
        cached = @@const_json
        cached = @@const_json = ConstJson.new(const, @size) unless cached && cached.for?(const, @size)
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

  # Rates stored as tenths in T samples: UInt32 holds 429 million per second, enough for
  # messages, UInt64 for bytes
  struct RateLogView(T)
    include Enumerable(Float64)

    # Rates up to here decode to Float64 exactly and print as one decimal
    MAX_TENTHS = (1_u64 << 51).to_f64

    def self.max_tenths : Float64
      Math.min(T::MAX.to_f64, MAX_TENTHS)
    end

    def self.encode(rate : Float64) : T
      T.new((rate * 10).round.clamp(0.0, max_tenths))
    end

    # Same as `encode((delta / interval).round(1))`, with one rounding
    def self.encode_delta(delta : UInt64, interval : Float64) : T
      T.new((delta / interval * 10).round.clamp(0.0, max_tenths))
    end

    def self.decode(tenths : Int) : Float64
      tenths / 10.0
    end

    def initialize(@tenths : StatLogView(T))
    end

    delegate size, to: @tenths

    def [](index : Int32) : Float64
      RateLogView(T).decode(@tenths[index])
    end

    def each(& : Float64 ->) : Nil
      @tenths.each { |t| yield RateLogView(T).decode(t) }
    end

    @@const_json : ConstJson(Float64)? = nil

    def to_json(json : JSON::Builder) : Nil
      if @tenths.constant?
        rate = RateLogView(T).decode(@tenths.const)
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
          if n > BUFFER - 24
            io.write_string(Slice.new(ptr, n))
            n = 0
          end
          unless first
            ptr[n] = ','.ord.to_u8
            n += 1
          end
          first = false
          n += tenths ? write_tenths(ptr + n, value.to_u64) : write_digits(ptr + n, value.to_u64)
        end
        ptr[n] = ']'.ord.to_u8
        io.write_string(Slice.new(ptr, n + 1))
      end
    end

    private def self.write_tenths(ptr : Pointer(UInt8), tenths : UInt64) : Int32
      len = write_digits(ptr, tenths // 10)
      ptr[len] = '.'.ord.to_u8
      ptr[len + 1] = 48_u8 &+ (tenths % 10).to_u8!
      len + 2
    end

    private def self.write_digits(ptr : Pointer(UInt8), value : UInt64) : Int32
      digits = uninitialized UInt8[20]
      i = 20
      loop do
        i -= 1
        digits[i] = 48_u8 &+ (value % 10).to_u8!
        value //= 10
        break if value.zero?
      end
      ptr.copy_from(digits.to_unsafe + i, 20 - i)
      20 - i
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
