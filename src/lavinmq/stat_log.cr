require "json"
require "../stdlib/json_builder"

module LavinMQ
  # A stat's last value, plus a ring of T samples on the GC heap while it changes. The ring
  # is headed by its capacity, so a view never reads past it
  struct StatColumn(T)
    getter last : T
    @run : Int32 = 0 # samples since the value last changed; in the padding of a 4-byte T
    @ring = Atomic(Pointer(T)).new(Pointer(T).null)

    def initialize(@last : T)
    end

    # Null while the column is its last value. Published after its header and fill, so a view
    # on another thread never pairs a ring with a stale capacity
    def ring : Pointer(T)
      @ring.get(:acquire)
    end

    @[AlwaysInline]
    def buffered? : Bool
      !@ring.get(:relaxed).null?
    end

    # Takes a ring on the first change, filled with the value so far; true when it did
    @[AlwaysInline]
    def record(value : T, tail : Int32, established : Bool, cap : Int32) : Bool
      ring = @ring.get(:relaxed)
      if value == @last
        if ring
          ring[tail] = value
          @run += 1
        end
        return false
      end
      taken = false
      if ring
        ring[tail] = value
      elsif established
        ring = StatColumn.alloc(cap, @last)
        ring[tail] = value
        @ring.set(ring, :release)
        taken = true
      end
      @run = 1
      @last = value
      taken
    end

    # Whether the whole window is the last value, so the ring reads the same without it
    def steady?(cap : Int32) : Bool
      buffered? && @run >= cap
    end

    def drop : Nil
      @ring.set(Pointer(T).null, :release)
    end

    # Keeps the newest samples, compacted to the front, as the ring position restarts at 0.
    # A view taken before keeps reading the old ring. True when the ring was dropped instead,
    # as a one-sample window is its last value
    def resize(head : Int32, size : Int32, old_cap : Int32, new_cap : Int32) : Bool
      old = @ring.get(:relaxed)
      return false unless old
      if new_cap <= 1
        drop
        return true
      end
      keep = Math.min(size, new_cap)
      ring = StatColumn.alloc(new_cap, @last)
      first = head + size - keep
      keep.times do |k|
        src = first + k
        src -= old_cap if src >= old_cap
        ring[k] = old[src]
      end
      @ring.set(ring, :release)
      false
    end

    def self.alloc(cap : Int32, fill : T) : Pointer(T)
      ring = Pointer(T).malloc(cap + 1)
      ring.value = T.new(cap)
      ring += 1
      Slice.new(ring, cap).fill(fill)
      ring
    end

    def self.capacity(ring : Pointer(T)) : Int32
      ring[-1].to_i32
    end
  end

  # Reads `const` without a ring, and never past the ring's capacity
  struct StatLogView(T)
    include Enumerable(T)

    @capacity = 0

    def initialize(@ring : Pointer(T), @head : Int32, @size : Int32, @const : T)
      @capacity = StatColumn.capacity(@ring) if @ring
    end

    getter const : T

    def constant? : Bool
      @ring.null?
    end

    def size : Int32
      @ring.null? ? @size : Math.min(@size, @capacity)
    end

    def [](index : Int32) : T
      raise IndexError.new if index < 0 || index >= size
      return @const if @ring.null?
      @ring[wrap(@head % @capacity + index)]
    end

    def each(& : T ->) : Nil
      n = size
      if @ring.null?
        n.times { yield @const }
        return
      end
      i = @head % @capacity
      n.times do
        yield @ring[i]
        i = wrap(i + 1)
      end
    end

    def to_json(json : JSON::Builder) : Nil
      StatJson.digits(json, self)
    end

    private def wrap(i : Int32) : Int32
      i >= @capacity ? i - @capacity : i
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

    def to_json(json : JSON::Builder) : Nil
      StatJson.tenths(json, @tenths)
    end
  end

  # Formats a log as JSON in a stack buffer, 9x faster per sample than JSON::Builder#number
  module StatJson
    BUFFER  = 1024
    ELEMENT =   24 # comma, 20 digits, point, digit

    def self.digits(json : JSON::Builder, view : StatLogView(T)) : Nil forall T
      {% raise "StatJson formats unsigned samples, not #{T}" unless T < Int::Unsigned %}
      write(json, view) { |ptr, value| write_digits(ptr, value.to_u64) }
    end

    def self.tenths(json : JSON::Builder, view : StatLogView(T)) : Nil forall T
      {% raise "StatJson formats unsigned samples, not #{T}" unless T < Int::Unsigned %}
      write(json, view) { |ptr, value| write_tenths(ptr, value.to_u64) }
    end

    private def self.write(json : JSON::Builder, view : StatLogView, &) : Nil
      json.raw do |io|
        buf = uninitialized UInt8[BUFFER]
        ptr = buf.to_unsafe
        ptr[0] = '['.ord.to_u8
        n = 1
        first = true
        view.each do |value|
          if n > BUFFER - ELEMENT
            io.write_string(Slice.new(ptr, n))
            n = 0
          end
          unless first
            ptr[n] = ','.ord.to_u8
            n += 1
          end
          first = false
          n += yield ptr + n, value
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
end
