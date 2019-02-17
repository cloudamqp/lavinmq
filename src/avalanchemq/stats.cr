require "./config"

module AvalancheMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w())

      {% for name in stats_keys %}
      @{{name.id}}_count = 0
      @{{name.id}}_rate = 0_f32
      @{{name.id}}_log = Deque(Float32).new(Config.instance.stats_log_size)
      {% end %}

      {% for name in log_keys %}
      @{{name.id}}_log = Deque(UInt32).new(Config.instance.stats_log_size)
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
        {% for name in stats_keys %}
          @{{name.id}}_log.shift if @{{name.id}}_log.size > Config.instance.stats_log_size
          @{{name.id}}_log.push @{{name.id}}_rate
          @{{name.id}}_rate = @{{name.id}}_count.to_f32 / (Config.instance.stats_interval / 1000)
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
