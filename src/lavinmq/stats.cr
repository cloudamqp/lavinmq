require "./config"
require "./stats_log"

module LavinMQ
  module Stats
    # How much each counter increased per tick
    class_property(counter_log : StatsLog(UInt32)) { StatsLog(UInt32).new(Config.instance.stats_log_size) }
    # Values of gauges per tick. There are only a few, so they get smaller chunks.
    class_property(gauge_log : StatsLog(Int64)) { StatsLog(Int64).new(Config.instance.stats_log_size, 64) }

    # Tick when the owner was created, the log of each series starts there
    @stats_log_start : Int64 = Stats.counter_log.tick

    # Defines a counter, a rate and a rate log (in `counter_log`) per key.
    # deliver_get is the sum of deliver, deliver_no_ack, get and get_no_ack,
    # so it's derived from those instead of tracked on its own.
    macro rate_stats(stats_keys)
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% derived = {"deliver_get" => %w[deliver deliver_no_ack get get_no_ack]} %}
      {% tracked = stats_keys.reject { |k| derived[k] } %}
      {% for name in tracked %}
        @{{ name.id }}_count = Atomic(UInt64).new(0_u64)
        @{{ name.id }}_count_prev = 0_u64
        @{{ name.id }}_rate = 0_f64
        @{{ name.id }}_log = StatsLog::Series.new

        def {{ name.id }}_count
          @{{ name.id }}_count.get(:relaxed)
        end
      {% end %}
      {% for name in stats_keys %}
        {% parts = derived[name] %}
        {% if parts %}
          {% for part in parts %}
            {% raise "rate_stats: #{name.id} requires #{part.id}" unless stats_keys.includes?(part) %}
          {% end %}
          @{{ name.id }}_rate = 0_f64

          def {{ name.id }}_count
            {{ parts.map { |p| "#{p.id}_count".id }.join(" + ").id }}
          end
        {% end %}

        # Rate per second at each tick, oldest first
        def {{ name.id }}_log : Array(Float64)
          stats_log = Stats.counter_log
          interval = Config.instance.stats_interval / 1000.0
          stats_log.read(stats_log.ticks_since(@stats_log_start),
            {{ (parts || [name]).map { |p| "@#{p.id}_log".id }.join(", ").id }}) do |increase|
            (increase / interval).round(1)
          end
        end

        # Adds the rate per second at each tick to *rates*, aligned at the latest tick
        def add_{{ name.id }}_log(rates : Array(Float64)) : Nil
          stats_log = Stats.counter_log
          interval = Config.instance.stats_interval / 1000.0
          stats_log.merge_into(rates, stats_log.ticks_since(@stats_log_start),
            {{ (parts || [name]).map { |p| "@#{p.id}_log".id }.join(", ").id }}) do |rate, increase|
            (rate + (increase / interval).round(1)).round(1)
          end
        end
      {% end %}

      def stats_details
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: {
              rate: @{{ name.id }}_rate,
              log: {{ name.id }}_log,
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
        stats_log = Stats.counter_log
        {% for name in tracked %}
          {{ name.id }}_count = @{{ name.id }}_count.get(:relaxed)
          {{ name.id }}_increase = {{ name.id }}_count - @{{ name.id }}_count_prev
          @{{ name.id }}_count_prev = {{ name.id }}_count
          @{{ name.id }}_rate = ({{ name.id }}_increase / interval).round(1)
          unless {{ name.id }}_increase.zero? # rows start at zero, so idle counters cost nothing
            @{{ name.id }}_log = stats_log.write(@{{ name.id }}_log, Stats.log_value({{ name.id }}_increase))
          end
        {% end %}
        {% for name in stats_keys %}
          {% if parts = derived[name] %}
            @{{ name.id }}_rate = (({{ parts.map { |p| "#{p.id}_increase".id }.join(" + ").id }}) / interval).round(1)
          {% end %}
        {% end %}
      end
    end

    # Defines a log of values (in `gauge_log`) per key, that the owner
    # stores once per tick with `log_<key>(value)`
    macro gauge_stats(gauge_keys)
      {% for name in gauge_keys %}
        @{{ name.id }}_log = StatsLog::Series.new

        # Value at each tick, oldest first
        def {{ name.id }}_log : Array(Int64)
          stats_log = Stats.gauge_log
          stats_log.read(stats_log.ticks_since(@stats_log_start), @{{ name.id }}_log, &.itself)
        end

        # Adds the value at each tick to *values*, aligned at the latest tick
        def add_{{ name.id }}_log(values : Array(Int64)) : Nil
          stats_log = Stats.gauge_log
          stats_log.merge_into(values, stats_log.ticks_since(@stats_log_start), @{{ name.id }}_log) do |value, sum|
            value + sum
          end
        end

        private def log_{{ name.id }}(value : Int) : Nil
          @{{ name.id }}_log = Stats.gauge_log.write(@{{ name.id }}_log, value.to_i64)
        end
      {% end %}
    end

    # How much a counter increased in a tick, as stored in `counter_log`.
    # Increases above UInt32::MAX are capped.
    def self.log_value(increase : Int) : UInt32
      increase.clamp(0, UInt32::MAX).to_u32
    end
  end
end
