require "http/server/handler"
require "../../version"
require "../../config"

module LavinMQ
  module HTTP
    # Ignores flushes (JSON::Builder flushes when its document ends), so a response
    # that fits the output buffer is sent with Content-Length instead of chunked.
    # Typed to the buffered output so forwarding inlines: the response forwards
    # each of JSON::Builder's many small writes, and one more call per write
    # through IO cost list endpoints 7%.
    class UnflushedOutput < IO
      def initialize(@output : ::HTTP::Server::Response::Output)
      end

      def read(slice : Bytes) : NoReturn
        raise "Can't read from HTTP::Server::Response"
      end

      def write(slice : Bytes) : Nil
        @output.write(slice)
      end

      def write_byte(byte : UInt8) : Nil
        @output.write_byte(byte)
      end

      def flush : Nil
      end

      def close : Nil
        @output.close
      end

      def closed? : Bool
        @output.closed?
      end
    end

    class ApiDefaultsHandler
      include ::HTTP::Handler
      Log = LavinMQ::Log.for "http.performance"

      def call(context)
        context.response.content_type = "application/json"
        output = context.response.output.as(::HTTP::Server::Response::Output)
        context.response.output = UnflushedOutput.new(output)
        # Advertise the server version on API responses; the management UI reads
        # it off any API response (see static/js/http.js) to show the version
        # without an extra request.
        context.response.headers["LavinMQ-Version"] = LavinMQ::VERSION
        context.response.headers["LavinMQ-Stats-Interval"] = Config.instance.stats_interval.to_s
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
