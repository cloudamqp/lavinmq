module AvalancheMQ
  module Stats
    macro rate_stats(stats_keys, log_keys = %w())

      {% for name in stats_keys %}
      @{{name.id}}_count = 0
      @{{name.id}}_rate = 0_f32
      @{{name.id}}_log = Array(Float32).new(Server.config.stats_log_size)
      {% end %}

      {% for name in log_keys %}
      @{{name.id}}_log = Array(UInt32).new(Server.config.stats_log_size)
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
          @{{name.id}}_log << @{{name.id}}_rate
          @{{name.id}}_log.shift if @{{name.id}}_log.size > Server.config.stats_log_size
          @{{name.id}}_rate = @{{name.id}}_count.to_f32 / (Server.config.stats_interval / 1000)
          @{{name.id}}_count = 0
        {% end %}
        {% for name in log_keys %}
          @{{name.id}}_log << {{name.id}}
          @{{name.id}}_log.shift if @{{name.id}}_log.size > Server.config.stats_log_size
        {% end %}
      end
    end
  end
end
