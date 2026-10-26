require "./config"
require "./stat_log"

module LavinMQ
  # Only the stats loop writes a log: `update_rates` and `release_retired` run there. Any fiber
  # may read a view or retire an object, and sees word-sized state a tick late at worst.
  module Stats
    @@retired = Array(Stats).new
    @@retired_lock = Mutex.new

    # Closed objects with buffers queue here, so the stats loop releases them, not the closer.
    # One without is left to the GC, not held on the heap until the next tick
    def self.retire(object : Stats) : Bool
      return false unless object.stats_log_buffered?
      @@retired_lock.synchronize { @@retired << object }
      true
    end

    def self.release_retired : Nil
      retired = @@retired_lock.synchronize do
        list = @@retired
        @@retired = Array(Stats).new
        list
      end
      retired.each &.stats_release
    end

    abstract def stats_log_buffered? : Bool
    abstract def stats_release : Nil

    # Columns share one ring and only take buffers while their values change. Message rates
    # are 4-byte samples, byte rates and gauges 8, each width in a pool of its own: slot 0 and 1
    macro rate_stats(stats_keys, log_keys = %w[])
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% rates = stats_keys.map do |k|
           if k.ends_with?("_oct")
             {name: k.id, ivar: "@#{k.id}_rate_log".id, type: UInt64, slot: 1}
           else
             {name: k.id, ivar: "@#{k.id}_rate_log".id, type: UInt32, slot: 0}
           end
         end %}
      {% gauges = log_keys.map { |k| {name: k.id, ivar: "@#{k.id}_log".id, type: UInt64, slot: 1} } %}
      {% columns = rates + gauges %}
      @_stats_blocks = StaticArray(Int32, 2).new(0) # buffered columns per width
      @_stats_high = StaticArray(Int32, 2).new(-1)  # highest ordinal of their blocks in each pool
      @_stats_lease : UInt32 = 0                    # tick their lease was last renewed at
      @_stats_log_capacity : Int32 = Config.instance.stats_log_size
      @_stats_log_head = Atomic(Int32).new(0) # read by views on any thread
      @_stats_log_size = Atomic(Int32).new(0)
      @_stats_steady_run : Int32 = 0 # trailing samples without a change; 0 while no column is buffered

      {% for c in rates %}
        @{{ c[:name] }}_count = Atomic(UInt64).new(0_u64)
        @{{ c[:name] }}_count_prev = 0_u64
        {{ c[:ivar] }} = StatColumn({{ c[:type] }}).new({{ c[:type] }}.zero)

        def {{ c[:name] }}_count
          @{{ c[:name] }}_count.get(:relaxed)
        end

        def {{ c[:name] }}_log : RateLogView({{ c[:type] }})
          RateLogView.new(_stats_view({{ c[:ivar] }}.last) { {{ c[:ivar] }}.block })
        end
      {% end %}
      {% for c in gauges %}
        {{ c[:ivar] }} = StatColumn(UInt64).new(0_u64)

        def {{ c[:name] }}_log : StatLogView(UInt64)
          _stats_view({{ c[:ivar] }}.last) { {{ c[:ivar] }}.block }
        end
      {% end %}

      def stats_log_buffered? : Bool
        @_stats_blocks.sum > 0
      end

      # Hands the buffers back; the log stays its last value. Past the lease the pool may have
      # taken them back already, so they're only forgotten
      def stats_release : Nil
        _stats_drop_buffers(@_stats_log_size.get(:relaxed), release: !_stats_expired?)
      end

      # Past the lease the pool may have handed the blocks out again
      private def _stats_expired? : Bool
        StatBlockPool.tick &- @_stats_lease > StatBlockPool::LEASE
      end

      # Retries if the column changed block while the view read its header. An expired log
      # reads its last value, as its block may hold another object's samples by now
      private def _stats_view(last : T, & : -> Pointer(T)) : StatLogView(T) forall T
        if stats_log_buffered? && _stats_expired?
          return StatLogView.new(Pointer(T).null, 0, @_stats_log_size.get(:acquire), last)
        end
        loop do
          block = yield
          Atomic.fence(:acquire)
          view = StatLogView.new(block, @_stats_log_head.get(:acquire), @_stats_log_size.get(:acquire), last)
          Atomic.fence(:acquire)
          return view if yield == block
        end
      end

      @[NoInline]
      private def _stats_alloc(fill : T, cap : Int32, slot : Int32) : Pointer(T) forall T
        block = StatBlockPool.for(cap, T).alloc(T)
        Slice.new(block, cap).fill(fill)
        @_stats_lease = StatBlockPool.tick unless stats_log_buffered?
        @_stats_blocks[slot] += 1
        @_stats_high[slot] = Math.max(@_stats_high[slot], StatBlockPool.ordinal(block))
        Atomic.fence(:release)
        block
      end

      def stats_details
        {
          {% for c in rates %}
            {{ c[:name] }}: {{ c[:name] }}_count,
            {{ c[:name] }}_details: {
              rate: RateLogView({{ c[:type] }}).decode({{ c[:ivar] }}.last),
              log:  {{ c[:name] }}_log,
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for c in rates %}
            {{ c[:name] }}: {{ c[:name] }}_count,
            {{ c[:name] }}_details: { rate: RateLogView({{ c[:type] }}).decode({{ c[:ivar] }}.last) },
          {% end %}
        }
      end

      # *interval* is the seconds since the last update: what the stats loop slept, which a
      # reload may have changed since
      def update_rates(interval : Float64 = Config.instance.stats_interval / 1000.0) : Nil
        cap = Config.instance.stats_log_size
        size = @_stats_log_size.get(:relaxed)
        if @_stats_steady_run == 0 && cap == @_stats_log_capacity && _stats_idle?
          @_stats_log_size.set(size + 1, :release) if size < cap
          return
        end
        _stats_keep_lease if stats_log_buffered?
        _stats_resize(cap) if cap != @_stats_log_capacity || _stats_scattered?
        head = @_stats_log_head.get(:relaxed)
        size = @_stats_log_size.get(:relaxed)
        tail = head + size
        tail -= cap if tail >= cap
        # A rate's first sample spans a partial interval (handshake, declare-time traffic), so its
        # history starts with the second; a one-sample log is its last value. A gauge's first
        # sample is a reading, kept once the next one differs
        established = size > 1 && cap > 1
        sampled = size > 0 && cap > 1
        changed = false

        {% for c in rates %}
          count = @{{ c[:name] }}_count.get(:relaxed)
          delta = count - @{{ c[:name] }}_count_prev
          tenths = {{ c[:type] }}.zero
          unless delta.zero? # idle objects must not dirty their cache lines
            @{{ c[:name] }}_count_prev = count
            tenths = RateLogView({{ c[:type] }}).encode_delta(delta, interval)
          end
          changed = true if {{ c[:ivar] }}.record(tenths, tail, established) { |fill| _stats_alloc(fill, cap, {{ c[:slot] }}) }
        {% end %}
        {% for c in gauges %}
          changed = true if {{ c[:ivar] }}.record({{ c[:name] }}.to_u64, tail, sampled) { |fill| _stats_alloc(fill, cap, {{ c[:slot] }}) }
        {% end %}

        buffered = @_stats_steady_run > 0 || (changed && stats_log_buffered?)
        unless buffered
          @_stats_log_size.set(size + 1, :release) if size < cap
          return
        end

        @_stats_steady_run = changed ? 1 : @_stats_steady_run + 1
        if @_stats_steady_run >= cap
          _stats_drop_buffers(cap)
        elsif size < cap
          @_stats_log_size.set(size + 1, :release)
        else
          head += 1
          head -= cap if head >= cap
          @_stats_log_head.set(head, :release)
        end
      end

      @[AlwaysInline]
      private def _stats_idle? : Bool
        {% for c in rates %}
          return false unless @{{ c[:name] }}_count.get(:relaxed) == @{{ c[:name] }}_count_prev && {{ c[:ivar] }}.last.zero?
        {% end %}
        {% for c in gauges %}
          return false unless {{ c[:name] }}.to_u64 == {{ c[:ivar] }}.last
        {% end %}
        true
      end

      # Past the lease the pool may have taken the buffers back, so they're forgotten
      private def _stats_keep_lease : Nil
        tick = StatBlockPool.tick
        age = tick &- @_stats_lease
        return if age < StatBlockPool::LEASE // 4
        if age > StatBlockPool::LEASE
          _stats_drop_buffers(@_stats_log_size.get(:relaxed), release: false)
        else
          {% for c in columns %}
            {{ c[:ivar] }}.renew(tick)
          {% end %}
          @_stats_lease = tick
        end
      end

      private def _stats_drop_buffers(size : Int32, release = true) : Nil
        cap = @_stats_log_capacity
        {% for c in columns %}
          if (block = {{ c[:ivar] }}.drop) && release
            Atomic.fence(:release)
            StatBlockPool.for(cap, {{ c[:type] }}).release(block)
          end
        {% end %}
        @_stats_blocks.fill(0)
        @_stats_high.fill(-1)
        @_stats_log_head.set(0, :release)
        @_stats_log_size.set(size, :release)
        @_stats_steady_run = 0
      end

      # A move takes every column along: the other width's blocks land at the lowest free
      # blocks of their pool, where a move would put them anyway
      private def _stats_scattered? : Bool
        cap = @_stats_log_capacity
        {% for type, slot in {UInt32, UInt64} %}
          if @_stats_blocks[{{ slot }}] > 0
            return true if StatBlockPool.for(cap, {{ type }}).relocate?(@_stats_high[{{ slot }}], @_stats_blocks[{{ slot }}])
          end
        {% end %}
        false
      end

      # Views keep reading the old blocks until they're handed out again. A move within the
      # same capacity keeps the ring's layout, so the head and size stay valid for the new block
      private def _stats_resize(cap : Int32) : Nil
        head = @_stats_log_head.get(:relaxed)
        size = @_stats_log_size.get(:relaxed)
        if stats_log_buffered?
          old_cap = @_stats_log_capacity
          blocks = StaticArray(Pointer(UInt8), {{ columns.size }}).new(Pointer(UInt8).null)
          begin
            {% for c, i in columns %}
              blocks[{{ i }}] = StatBlockPool.for(cap, {{ c[:type] }}).alloc if {{ c[:ivar] }}.buffered?
            {% end %}
          rescue ex
            {% for c, i in columns %}
              if block = blocks[{{ i }}]
                StatBlockPool.for(cap, {{ c[:type] }}).release(block)
              end
            {% end %}
            _stats_drop_buffers(Math.min(size, cap))
            @_stats_log_capacity = cap
            raise ex
          end
          @_stats_high.fill(-1)
          {% for c, i in columns %}
            if block = blocks[{{ i }}].as(Pointer({{ c[:type] }}))
              @_stats_high[{{ c[:slot] }}] = Math.max(@_stats_high[{{ c[:slot] }}], StatBlockPool.ordinal(block))
              StatBlockPool.for(old_cap, {{ c[:type] }}).release({{ c[:ivar] }}.move(block, old_cap, head, size, cap))
            end
          {% end %}
          @_stats_lease = StatBlockPool.tick
        end
        return if cap == @_stats_log_capacity
        @_stats_log_head.set(0, :release)
        @_stats_log_size.set(Math.min(size, cap), :release)
        @_stats_log_capacity = cap
      end
    end
  end
end
