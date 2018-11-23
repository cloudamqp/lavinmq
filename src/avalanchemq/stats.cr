module AvalancheMQ
  module Stats
    macro rate_stats(keys)
      {% for name in keys %}
      @{{name.id}}_count = 0
      @{{name.id}}_rate = 0_f32
      {% end %}

      def stats_details
        { {% for name in keys %}
          {{name.id}}_details: { rate: @{{name.id}}_rate },
        {% end %} }
      end

      def update_rates
        {% for name in keys %}
          @{{name.id}}_rate = @{{name.id}}_count.to_f32 / (Server.config.stats_interval / 1000)
          @{{name.id}}_count = 0
        {% end %}
      end
    end
  end
end
