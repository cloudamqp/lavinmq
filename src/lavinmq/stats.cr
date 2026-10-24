require "./config"
require "./stat_log"

module LavinMQ
  module Stats
    # Columns share one ring and only take buffers while their values change
    macro rate_stats(stats_keys, log_keys = %w[])
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% columns = stats_keys.map { |k| "@#{k.id}_rate_log".id } + log_keys.map { |k| "@#{k.id}_log".id } %}
      @_stats_blocks : Int32 = 0 # buffered columns
      @_stats_high : Int32 = -1  # highest ordinal of their blocks in the pool
      @_stats_lease : UInt32 = 0 # tick their lease was last renewed at
      @_stats_log_capacity : Int32 = Config.instance.stats_log_size
      @_stats_log_head : Int32 = 0
      @_stats_log_size : Int32 = 0
      @_stats_steady_run : Int32 = 0 # trailing samples without a change; 0 while no column is buffered

      {% for name, i in stats_keys %}
        @{{ name.id }}_count = Atomic(UInt64).new(0_u64)
        @{{ name.id }}_count_prev = 0_u64
        @{{ name.id }}_rate_log = StatColumn.new(0_u64)

        def {{ name.id }}_count
          @{{ name.id }}_count.get(:relaxed)
        end

        def {{ name.id }}_log : RateLogView
          RateLogView.new(_stats_view(@{{ name.id }}_rate_log.last) { @{{ name.id }}_rate_log.block })
        end
      {% end %}
      {% for name, i in log_keys %}
        @{{ name.id }}_log = StatColumn.new(0_u64)

        def {{ name.id }}_log : StatLogView
          _stats_view(@{{ name.id }}_log.last) { @{{ name.id }}_log.block }
        end
      {% end %}

      def stats_log_buffered? : Bool
        @_stats_blocks > 0
      end

      # Retries if the column changed block while the view read its header
      private def _stats_view(last : UInt64, & : -> Pointer(UInt64)) : StatLogView
        loop do
          block = yield
          Atomic.fence(:acquire)
          view = StatLogView.new(block, @_stats_log_head, @_stats_log_size, last)
          Atomic.fence(:acquire)
          return view if yield == block
        end
      end

      @[NoInline]
      private def _stats_alloc(fill : UInt64, cap : Int32) : Pointer(UInt64)
        block = StatBlockPool.for(cap).alloc
        Slice.new(block, cap).fill(fill)
        @_stats_lease = StatBlockPool.tick if @_stats_blocks.zero?
        @_stats_blocks += 1
        @_stats_high = Math.max(@_stats_high, StatBlockPool.ordinal(block))
        Atomic.fence(:release)
        block
      end

      def stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: {
              rate: RateLogView.decode(@{{ name.id }}_rate_log.last),
              log:  {{ name.id }}_log,
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: { rate: RateLogView.decode(@{{ name.id }}_rate_log.last) },
          {% end %}
        }
      end

      def update_rates : Nil
        cap = Config.instance.stats_log_size
        if @_stats_steady_run == 0 && cap == @_stats_log_capacity && _stats_idle?
          @_stats_log_size += 1 if @_stats_log_size < cap
          return
        end
        interval = Config.instance.stats_interval / 1000.0
        _stats_keep_lease if stats_log_buffered?
        _stats_resize(cap) if cap != @_stats_log_capacity || _stats_scattered?
        tail = @_stats_log_head + @_stats_log_size
        tail -= cap if tail >= cap
        established = @_stats_log_size > 0 && cap > 1 # a one-sample log is its last value
        changed = false

        {% for name, i in stats_keys %}
          count = @{{ name.id }}_count.get(:relaxed)
          delta = count - @{{ name.id }}_count_prev
          tenths = 0_u64
          unless delta.zero? # idle objects must not dirty their cache lines
            @{{ name.id }}_count_prev = count
            tenths = RateLogView.encode_delta(delta, interval)
          end
          changed = true if @{{ name.id }}_rate_log.record(tenths, tail, established) { |fill| _stats_alloc(fill, cap) }
        {% end %}
        {% for name, i in log_keys %}
          changed = true if @{{ name.id }}_log.record({{ name.id }}.to_u64, tail, established) { |fill| _stats_alloc(fill, cap) }
        {% end %}

        buffered = @_stats_steady_run > 0 || (changed && stats_log_buffered?)
        unless buffered
          @_stats_log_size += 1 if @_stats_log_size < cap
          return
        end

        @_stats_steady_run = changed ? 1 : @_stats_steady_run + 1
        if @_stats_steady_run >= cap
          _stats_drop_buffers(cap)
        elsif @_stats_log_size < cap
          @_stats_log_size += 1
        else
          @_stats_log_head += 1
          @_stats_log_head -= cap if @_stats_log_head >= cap
        end
      end

      @[AlwaysInline]
      private def _stats_idle? : Bool
        {% for name in stats_keys %}
          return false unless @{{ name.id }}_count.get(:relaxed) == @{{ name.id }}_count_prev && @{{ name.id }}_rate_log.last.zero?
        {% end %}
        {% for name in log_keys %}
          return false unless {{ name.id }}.to_u64 == @{{ name.id }}_log.last
        {% end %}
        true
      end

      # Past the lease the pool may have taken the buffers back, so they're forgotten
      private def _stats_keep_lease : Nil
        tick = StatBlockPool.tick
        age = tick &- @_stats_lease
        return if age < StatBlockPool::LEASE // 4
        if age > StatBlockPool::LEASE
          _stats_drop_buffers(@_stats_log_size, release: false)
        else
          {% for column in columns %}
            {{ column }}.renew(tick)
          {% end %}
          @_stats_lease = tick
        end
      end

      private def _stats_drop_buffers(size : Int32, release = true) : Nil
        pool = StatBlockPool.for(@_stats_log_capacity)
        {% for column in columns %}
          if (block = {{ column }}.drop) && release
            Atomic.fence(:release)
            pool.release(block)
          end
        {% end %}
        @_stats_blocks = 0
        @_stats_high = -1
        @_stats_log_head = 0
        @_stats_log_size = size
        @_stats_steady_run = 0
      end

      private def _stats_scattered? : Bool
        stats_log_buffered? && StatBlockPool.for(@_stats_log_capacity).relocate?(@_stats_high, @_stats_blocks)
      end

      # Views keep reading the old blocks until they're handed out again
      private def _stats_resize(cap : Int32) : Nil
        if stats_log_buffered?
          old_pool = StatBlockPool.for(@_stats_log_capacity)
          pool = StatBlockPool.for(cap)
          blocks = StaticArray(Pointer(UInt64), {{ columns.size }}).new(Pointer(UInt64).null)
          begin
            {% for column, i in columns %}
              blocks[{{ i }}] = pool.alloc if {{ column }}.buffered?
            {% end %}
          rescue ex
            blocks.each { |block| pool.release(block) if block }
            _stats_drop_buffers(Math.min(@_stats_log_size, cap))
            @_stats_log_capacity = cap
            raise ex
          end
          @_stats_high = -1
          {% for column, i in columns %}
            if block = blocks[{{ i }}]
              @_stats_high = Math.max(@_stats_high, StatBlockPool.ordinal(block))
              old_pool.release({{ column }}.move(block, @_stats_log_capacity, @_stats_log_head, @_stats_log_size, cap))
            end
          {% end %}
          @_stats_lease = StatBlockPool.tick
        end
        @_stats_log_head = 0
        @_stats_log_size = Math.min(@_stats_log_size, cap)
        @_stats_log_capacity = cap
      end
    end
  end
end
