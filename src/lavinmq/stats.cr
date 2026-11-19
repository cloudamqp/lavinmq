require "./config"
require "./stat_log"

module LavinMQ
  # Only the stats loop writes a log. Any fiber may read a view; one on another thread may
  # catch a tick in progress and see a full ring rotated by one sample. The GC keeps a view's
  # ring however the column moves on
  module Stats
    # Columns share one ring position and only take a ring while their values change.
    # Message rates are 4-byte samples, byte rates 8 and gauges their log_type; the vhost
    # totals need UInt64. deliver_get is the sum of deliver, deliver_no_ack, get and
    # get_no_ack, so it's derived from those instead of counted
    macro rate_stats(stats_keys, log_keys = %w[], log_type = UInt32)
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% derived = {"deliver_get" => %w[deliver deliver_no_ack get get_no_ack]} %}
      {% rates = stats_keys.map do |k|
           {name: k.id, ivar: "@#{k.id}_rate_log".id, type: k.ends_with?("_oct") ? UInt64 : UInt32, parts: derived[k]}
         end %}
      {% tracked = rates.reject { |c| c[:parts] } %}
      {% gauges = log_keys.map { |k| {name: k.id, ivar: "@#{k.id}_log".id, type: log_type} } %}
      {% columns = rates + gauges %}
      @_stats_log_capacity : Int32 = Config.instance.stats_log_size
      @_stats_buffered = 0 # columns holding a ring
      @_stats_log_head = Atomic(Int32).new(0) # read by views on any thread
      @_stats_log_size = Atomic(Int32).new(0)
      @_stats_baselined = false # the first tick is no sample, see update_rates

      {% for c in tracked %}
        @{{ c[:name] }}_count = Atomic(UInt64).new(0_u64)
        @{{ c[:name] }}_count_prev = 0_u64

        def {{ c[:name] }}_count
          @{{ c[:name] }}_count.get(:relaxed)
        end
      {% end %}
      {% for c in rates %}
        {{ c[:ivar] }} = StatColumn({{ c[:type] }}).new({{ c[:type] }}.zero)
        {% parts = c[:parts] %}
        {% if parts %}
          {% for part in parts %}
            {% raise "rate_stats: #{c[:name]} is derived from #{part.id}, which is not among the keys" unless stats_keys.includes?(part) %}
          {% end %}

          # The sum of its parts, nothing counts it on its own
          def {{ c[:name] }}_count
            {{ parts.map { |p| "#{p.id}_count".id }.join(" + ").id }}
          end
        {% end %}

        def {{ c[:name] }}_log : RateLogView({{ c[:type] }})
          RateLogView.new(_stats_view({{ c[:ivar] }}.ring, {{ c[:ivar] }}.last))
        end
      {% end %}
      {% for c in gauges %}
        {{ c[:ivar] }} = StatColumn({{ c[:type] }}).new({{ c[:type] }}.zero)

        def {{ c[:name] }}_log : StatLogView({{ c[:type] }})
          _stats_view({{ c[:ivar] }}.ring, {{ c[:ivar] }}.last)
        end
      {% end %}

      def stats_log_buffered? : Bool
        @_stats_buffered > 0
      end

      private def _stats_view(ring : Pointer(T), last : T) : StatLogView(T) forall T
        size = @_stats_log_size.get(:acquire)
        head = @_stats_log_head.get(:acquire)
        StatLogView.new(ring, head, size, last)
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
        _stats_resize(cap) if cap != @_stats_log_capacity
        size = @_stats_log_size.get(:relaxed)
        if !stats_log_buffered? && _stats_idle?
          if !@_stats_baselined
            @_stats_baselined = true
          elsif size < cap
            @_stats_log_size.set(size + 1, :release)
          end
          return
        end
        head = @_stats_log_head.get(:relaxed)
        tail = head + size
        tail -= cap if tail >= cap
        # An object's first tick spans a partial interval (handshake, declare-time traffic), so
        # it only sets the baseline: the values are current, but the log starts with the next
        sampled = size > 0 && cap > 1

        {% for c in tracked %}
          count = @{{ c[:name] }}_count.get(:relaxed)
          {{ c[:name] }}_delta = count - @{{ c[:name] }}_count_prev
          @{{ c[:name] }}_count_prev = count unless {{ c[:name] }}_delta.zero? # idle objects must not dirty their cache lines
        {% end %}
        {% for c in rates %}
          {% parts = c[:parts] %}
          {% if parts %}
            {{ c[:name] }}_delta = {{ parts.map { |p| "#{p.id}_delta".id }.join(" + ").id }}
          {% end %}
          tenths = {{ c[:name] }}_delta.zero? ? {{ c[:type] }}.zero : RateLogView({{ c[:type] }}).encode_delta({{ c[:name] }}_delta, interval)
          @_stats_buffered += 1 if {{ c[:ivar] }}.record(tenths, tail, sampled, cap)
        {% end %}
        {% for c in gauges %}
          @_stats_buffered += 1 if {{ c[:ivar] }}.record({{ c[:name] }}, tail, sampled, cap)
        {% end %}

        unless @_stats_baselined
          @_stats_baselined = true
          return
        end
        if size < cap
          @_stats_log_size.set(size + 1, :release)
        else
          head += 1
          head -= cap if head >= cap
          @_stats_log_head.set(head, :release)
        end
        # A column whose whole window is its last value reads the same without its ring
        {% for c in columns %}
          if {{ c[:ivar] }}.steady?(cap)
            {{ c[:ivar] }}.drop
            @_stats_buffered -= 1
          end
        {% end %}
      end

      @[AlwaysInline]
      private def _stats_idle? : Bool
        {% for c in tracked %}
          return false unless @{{ c[:name] }}_count.get(:relaxed) == @{{ c[:name] }}_count_prev
        {% end %}
        {% for c in rates %}
          return false unless {{ c[:ivar] }}.last.zero?
        {% end %}
        {% for c in gauges %}
          return false unless {{ c[:name] }} == {{ c[:ivar] }}.last
        {% end %}
        true
      end

      # A reloaded stats_log_size: the newest samples move to the front of new rings
      private def _stats_resize(cap : Int32) : Nil
        head = @_stats_log_head.get(:relaxed)
        size = @_stats_log_size.get(:relaxed)
        {% for c in columns %}
          @_stats_buffered -= 1 if {{ c[:ivar] }}.resize(head, size, @_stats_log_capacity, cap)
        {% end %}
        @_stats_log_head.set(0, :release)
        @_stats_log_size.set(Math.min(size, cap), :release)
        @_stats_log_capacity = cap
      end
    end
  end
end
