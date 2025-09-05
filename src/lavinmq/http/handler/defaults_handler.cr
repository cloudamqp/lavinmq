require "http/server/handler"

module LavinMQ
  module HTTP
    class ApiDefaultsHandler
      include ::HTTP::Handler
      Log = LavinMQ::Log.for "http.performance"

      def call(context)
        context.response.content_type = "application/json"
        {% if flag?(:release) %}
          call_next(context)
        {% else %}
          elapsed = Time::Span.new
          mem = Benchmark.memory do
            elapsed = Benchmark.realtime do
              call_next(context)
            end
          end
          L.info request: "#{context.request.path}?#{context.request.query}", memory: mem.humanize_bytes, elapsed: "#{elapsed.total_milliseconds.to_i}ms"
        {% end %}
      end
    end
  end
end
