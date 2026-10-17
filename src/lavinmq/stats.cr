require "./stats/striped_counters"

module LavinMQ
  module Stats
    # Defines a `<name>_count` counter, with a rate and a rate log that
    # `update_rates` maintains, for each key.
    #
    # Counters are bumped with `<name>_count_add!(n)`. By default that's a
    # relaxed load and store (a plain mov, no locked instruction), so it is
    # only for counters with a single concurrent writer: a connection's read
    # loop, or callers that all hold the same lock. Anything written from
    # several fibers at once must use `<name>_count_atomic_add!` instead.
    #
    # With `striped: true` every counter is kept in one stripe per thread
    # (see StripedCounters) and `_add!` is safe from any thread. The stripes
    # are allocated on the first add, so idle objects cost one pointer.
    # `pad_after: "key"` puts 64 bytes after that key's fields, to keep the
    # keys before it (written by one thread) off the cache line of the keys
    # after it (written by another).
    macro rate_stats(stats_keys, log_keys = %w[], striped = false, pad_after = nil)
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% if striped %}
        @rate_stats_stripes = Atomic(Stats::StripedCounters?).new(nil)

        private def rate_stats_stripes : Stats::StripedCounters
          @rate_stats_stripes.get(:acquire) || begin
            stripes = Stats::StripedCounters.new({{ stats_keys.size }})
            _, ok = @rate_stats_stripes.compare_and_set(nil, stripes, :acquire_release, :acquire)
            ok ? stripes : @rate_stats_stripes.get(:acquire).not_nil!
          end
        end

        {% for name, i in stats_keys %}
          @{{ name.id }}_count_prev = 0_u64
          @{{ name.id }}_rate = 0_f64
          @{{ name.id }}_log = Deque(Float64).new(Config.instance.stats_log_size)

          def {{ name.id }}_count : UInt64
            @rate_stats_stripes.get(:acquire).try(&.get({{ i }})) || 0_u64
          end

          private def {{ name.id }}_count_add!(n = 1) : Nil
            rate_stats_stripes.add({{ i }}, n.to_u64)
          end

          private def {{ name.id }}_count_sub!(n = 1) : Nil
            rate_stats_stripes.sub({{ i }}, n.to_u64)
          end
        {% end %}
      {% else %}
        {% for name in stats_keys %}
          @{{ name.id }}_count = Atomic(UInt64).new(0_u64)
          @{{ name.id }}_count_prev = 0_u64
          @{{ name.id }}_rate = 0_f64
          @{{ name.id }}_log = Deque(Float64).new(Config.instance.stats_log_size)

          def {{ name.id }}_count : UInt64
            @{{ name.id }}_count.get(:relaxed)
          end

          # Only when the caller is the sole concurrent writer, or holds the
          # lock every other writer holds
          private def {{ name.id }}_count_add!(n = 1) : Nil
            @{{ name.id }}_count.set(@{{ name.id }}_count.get(:relaxed) &+ n, :relaxed)
          end

          private def {{ name.id }}_count_atomic_add!(n = 1) : Nil
            @{{ name.id }}_count.add(n.to_u64, :relaxed)
          end

          {% if pad_after == name %}
            @{{ name.id }}_pad = StaticArray(UInt8, 64).new(0_u8)
          {% end %}
        {% end %}
      {% end %}
      {% for name in log_keys %}
        @{{ name.id }}_log = Deque(UInt32).new(Config.instance.stats_log_size)
        getter {{ name.id }}_log
      {% end %}

      def stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: {
              rate: @{{ name.id }}_rate,
              log: @{{ name.id }}_log
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: { rate: @{{ name.id }}_rate },
          {% end %}
        }
      end

      def update_rates : Nil
        interval = Config.instance.stats_interval / 1000.0
        log_size = Config.instance.stats_log_size
        {% for name in stats_keys %}
          until @{{ name.id }}_log.size < log_size
            @{{ name.id }}_log.shift
          end
          {{ name.id }}_count = {{ name.id }}_count()
          @{{ name.id }}_rate = (({{ name.id }}_count - @{{ name.id }}_count_prev) / interval).round(1)
          @{{ name.id }}_log.push @{{ name.id }}_rate
          @{{ name.id }}_count_prev = {{ name.id }}_count
        {% end %}
        {% for name in log_keys %}
          until @{{ name.id }}_log.size < log_size
            @{{ name.id }}_log.shift
          end
          @{{ name.id }}_log.push {{ name.id }}
        {% end %}
      end
    end
  end
end
