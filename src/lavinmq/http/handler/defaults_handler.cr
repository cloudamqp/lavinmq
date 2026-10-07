require "http/server/handler"
require "../../version"

module LavinMQ
  module HTTP
    class ApiDefaultsHandler
      include ::HTTP::Handler
      Log = LavinMQ::Log.for "http.performance"

      def call(context)
        context.response.content_type = "application/json"
        # Advertise the server version on API responses; the management UI reads
        # it off any API response (see static/js/http.js) to show the version
        # without an extra request.
        context.response.headers["LavinMQ-Version"] = LavinMQ::VERSION
        {% if flag?(:release) %}
          call_next(context)
        {% else %}
          elapsed = Time::Span.new
          mem = Benchmark.memory do
            elapsed = Benchmark.realtime do
              call_next(context)
            end
          end
          Log.info { "request=#{context.request.path}?#{context.request.query} memory=#{mem.humanize_bytes} elapsed=#{elapsed.total_milliseconds.to_i}ms" }
        {% end %}
      end
    end
  end
end
