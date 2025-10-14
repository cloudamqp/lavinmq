require "./config"
require "./ring_buffer"

module LavinMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w())
      {% for name in stats_keys %}
        @{{name.id}}_count = Atomic(UInt64).new(0_u64)
        @{{name.id}}_count_prev = 0_u64
        @{{name.id}}_rate = 0_f64
        @{{name.id}}_log = RingBuffer(Float64).new(Config.instance.stats_log_size)
        def {{name.id}}_count
          @{{name.id}}_count.get(:relaxed)
        end
      {% end %}
      {% for name in log_keys %}
        @{{name.id}}_log = RingBuffer(UInt32).new(Config.instance.stats_log_size)
        def {{name.id}}_log
          @{{name.id}}_log.to_a
        end
      {% end %}

      def stats_details
        {
          {% for name in stats_keys %}
            {{name.id}}: {{name.id}}_count,
            {{name.id}}_details: {
              rate: @{{name.id}}_rate,
              log: @{{name.id}}_log.to_a
            },
          {% end %}
        }
      end

      # Like stats_details but without log
      def current_stats_details
        {
          {% for name in stats_keys %}
            {{name.id}}: {{name.id}}_count,
            {{name.id}}_details: { rate: @{{name.id}}_rate },
          {% end %}
        }
      end

      def update_rates : Nil
        interval = Config.instance.stats_interval // 1000
        {% for name in stats_keys %}
          {{name.id}}_count = @{{name.id}}_count.get(:relaxed)
          @{{name.id}}_rate = (({{name.id}}_count - @{{name.id}}_count_prev) / interval).round(1)
          @{{name.id}}_log.push @{{name.id}}_rate
          @{{name.id}}_count_prev = {{name.id}}_count
        {% end %}
        {% for name in log_keys %}
          @{{name.id}}_log.push {{name.id}}
        {% end %}
      end
    end
  end
end
