require "./config"

module LavinMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w())
      {% for name in stats_keys %}
        @{{name.id}}_count = 0_u64
        @{{name.id}}_count_prev = 0_u64
        @{{name.id}}_rate = 0_f64
        @{{name.id}}_log = Deque(Float64).new(Config.instance.stats_log_size)
        getter {{name.id}}_count
      {% end %}
      {% for name in log_keys %}
        @{{name.id}}_log = Deque(UInt32).new(Config.instance.stats_log_size)
        getter {{name.id}}_log
      {% end %}

      def stats_details
        {
          {% for name in stats_keys %}
            {{name.id}}: @{{name.id}}_count,
            {{name.id}}_details: {
              rate: @{{name.id}}_rate,
              log: @{{name.id}}_log
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for name in stats_keys %}
            {{name.id}}: @{{name.id}}_count,
              {{name.id}}_details: {
              rate: @{{name.id}}_rate,
            },
          {% end %}
        }
      end

      def update_rates : Nil
        interval = Config.instance.stats_interval // 1000
        log_size = Config.instance.stats_log_size
        {% for name in stats_keys %}
          until @{{name.id}}_log.size < log_size
            @{{name.id}}_log.shift
          end
          @{{name.id}}_rate = ((@{{name.id}}_count - @{{name.id}}_count_prev) / interval).round(1)
          @{{name.id}}_log.push @{{name.id}}_rate
          @{{name.id}}_count_prev = @{{name.id}}_count
        {% end %}
        {% for name in log_keys %}
          until @{{name.id}}_log.size < log_size
            @{{name.id}}_log.shift
          end
          @{{name.id}}_log.push {{name.id}}
        {% end %}
      end
    end
  end
end
