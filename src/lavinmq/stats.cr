require "./stats_log"

module LavinMQ
  module Stats
    # Defines a counter, a rate and a rate history (in `StatsLog`) per key.
    # deliver_get is the sum of deliver, deliver_no_ack, get and get_no_ack,
    # so it's derived from those instead of tracked on its own.
    macro rate_stats(stats_keys)
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% derived = {"deliver_get" => %w[deliver deliver_no_ack get get_no_ack]} %}
      {% tracked = stats_keys.reject { |k| derived[k] } %}
      @stats_log_start : Int64 = StatsLog.instance.tick
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
      {% end %}

      def stats_details
        stats_log = StatsLog.instance
        log_size = stats_log.ticks_since(@stats_log_start)
        {
          {% for name in stats_keys %}
            {{ name.id }}: {{ name.id }}_count,
            {{ name.id }}_details: {
              rate: @{{ name.id }}_rate,
              {% if parts = derived[name] %}
                log: stats_log.read(log_size, {{ parts.map { |p| "@#{p.id}_log".id }.join(", ").id }}),
              {% else %}
                log: stats_log.read(log_size, @{{ name.id }}_log),
              {% end %}
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
        stats_log = StatsLog.instance
        {% for name in tracked %}
          {{ name.id }}_count = @{{ name.id }}_count.get(:relaxed)
          {{ name.id }}_rate = ({{ name.id }}_count - @{{ name.id }}_count_prev) / interval
          @{{ name.id }}_count_prev = {{ name.id }}_count
          @{{ name.id }}_rate = {{ name.id }}_rate.round(1)
          unless {{ name.id }}_rate.zero?
            @{{ name.id }}_log = stats_log.write(@{{ name.id }}_log, {{ name.id }}_rate)
          end
        {% end %}
        {% for name in stats_keys %}
          {% if parts = derived[name] %}
            @{{ name.id }}_rate = ({{ parts.map { |p| "#{p.id}_rate".id }.join(" + ").id }}).round(1)
          {% end %}
        {% end %}
      end
    end
  end
end
