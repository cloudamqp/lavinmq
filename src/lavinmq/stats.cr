module LavinMQ
  module Stats
    # log_keys are gauges sampled as log_type; the vhost totals need UInt64
    macro rate_stats(stats_keys, log_keys = %w[], log_type = UInt32)
      {% stats_keys = stats_keys.resolve if stats_keys.is_a?(Path) %}
      {% derived = {"deliver_get" => %w[deliver deliver_no_ack get get_no_ack]} %}
      {% tracked = stats_keys.reject { |k| derived[k] } %}
      {% for name in tracked %}
        @{{ name.id }}_count = Atomic(UInt64).new(0_u64)
        @{{ name.id }}_count_prev = 0_u64

        def {{ name.id }}_count
          @{{ name.id }}_count.get(:relaxed)
        end
      {% end %}
      {% for name in stats_keys %}
        @{{ name.id }}_rate = 0_f64
        @{{ name.id }}_log = Deque(Float64).new(Config.instance.stats_log_size)
        {% parts = derived[name] %}
        {% if parts %}
          {% for part in parts %}
            {% raise "rate_stats: #{name.id} is derived from #{part.id}, which is not among the keys" unless stats_keys.includes?(part) %}
          {% end %}

          # The sum of its parts, nothing counts it on its own
          def {{ name.id }}_count
            {{ parts.map { |p| "#{p.id}_count".id }.join(" + ").id }}
          end
        {% end %}
      {% end %}
      {% for name in log_keys %}
        @{{ name.id }}_log = Deque({{ log_type }}).new(Config.instance.stats_log_size)
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

      def update_rates(interval : Float64 = Config.instance.stats_interval / 1000.0) : Nil
        log_size = Config.instance.stats_log_size
        {% for name in tracked %}
          {{ name.id }}_count = @{{ name.id }}_count.get(:relaxed)
          {{ name.id }}_increase = {{ name.id }}_count - @{{ name.id }}_count_prev
          @{{ name.id }}_count_prev = {{ name.id }}_count
        {% end %}
        {% for name in stats_keys %}
          {% parts = derived[name] %}
          {% if parts %}
            {{ name.id }}_increase = {{ parts.map { |p| "#{p.id}_increase".id }.join(" + ").id }}
          {% end %}
          until @{{ name.id }}_log.size < log_size
            @{{ name.id }}_log.shift
          end
          @{{ name.id }}_rate = ({{ name.id }}_increase / interval).round(1)
          @{{ name.id }}_log.push @{{ name.id }}_rate
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
