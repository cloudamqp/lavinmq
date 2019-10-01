require "./config"

module AvalancheMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w())

      {% for name in stats_keys %}
      @{{name.id}}_count = 0_u64
      @{{name.id}}_rate = 0_f64
      @{{name.id}}_log = Deque(Float64).new(Config.instance.stats_log_size)
      {% end %}

      {% for name in log_keys %}
      @{{name.id}}_log = Deque(UInt64).new(Config.instance.stats_log_size)
      getter {{name.id}}_log
      {% end %}

      def stats_details
        { {% for name in stats_keys %}
          {{name.id}}_details: {
            rate: @{{name.id}}_rate,
            log: @{{name.id}}_log
          },
        {% end %} }
      end

      def update_rates
        interval = Config.instance.stats_interval / 1000_f64
        {% for name in stats_keys %}
          @{{name.id}}_log.shift if @{{name.id}}_log.size > Config.instance.stats_log_size
          @{{name.id}}_log.push @{{name.id}}_rate
          @{{name.id}}_rate = @{{name.id}}_count / interval
          @{{name.id}}_count = 0
        {% end %}
        {% for name in log_keys %}
          @{{name.id}}_log.shift if @{{name.id}}_log.size >= Config.instance.stats_log_size
          @{{name.id}}_log.push {{name.id}}
        {% end %}
      end
    end
  end
end
