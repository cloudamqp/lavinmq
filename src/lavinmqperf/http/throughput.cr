require "http/client"
require "json"
require "wait_group"
require "base64"
require "../perf"

module LavinMQPerf
  module HTTP
    class Throughput < Perf
      LATENCY_RESERVOIR_SIZE = 128 * 1024
      BASE_INSTANT           = Time.instant
      @tls : OpenSSL::SSL::Context::Client? = nil

      @publishers = 1
      @consumers = 1
      @size = 16
      @verify = false
      @exchange = ""
      @queue = "perf-test"
      @routing_key = "perf-test"
      @ack = 0
      @rate = 0
      @consume_rate = 0
      @persistent = false
      @quiet = false
      @json_output = false
      @timeout = Time::Span.zero
      @pmessages = 0
      @cmessages = 0
      @properties = {} of String => JSON::Any
      @random_bodies = false
      @queue_pattern : String? = nil
      @queue_pattern_from = 1
      @queue_pattern_to = 1
      @measure_latency = false
      @latencies = Array(Float64).new(LATENCY_RESERVOIR_SIZE)
      @latencies_mutex = Mutex.new
      @last_latencies = Array(Float64).new
      @latencies_count : UInt64 = 0
      @random = Random.new
      @vhost = "/"
      @username = "guest"
      @password = "guest"
      @host = "localhost"
      @port : Int32? = nil
      @insecure = false

      def initialize(io : IO = STDOUT)
        super(io)
        @parser.on("-x publishers", "--publishers=number", "Number of publishers (default 1)") do |v|
          @publishers = v.to_i
        end
        @parser.on("-y consumers", "--consumers=number", "Number of consumers (default 1)") do |v|
          @consumers = v.to_i
        end
        @parser.on("-s msgsize", "--size=bytes", "Size of each message (default 16 bytes)") do |v|
          @size = v.to_i
        end
        @parser.on("-V", "--verify", "Verify the message body") do
          @verify = true
        end
        @parser.on("-a MESSAGES", "--ack MESSAGES", "Ack after X consumed messages (default 0)") do |v|
          @ack = v.to_i
        end
        @parser.on("-u queue", "--queue=name", "Queue name (default perf-test)") do |v|
          @queue = v
          @routing_key = v
        end
        @parser.on("-k routing-key", "--routing-key=name", "Routing key (default queue name)") do |v|
          @routing_key = v
        end
        @parser.on("-e exchange", "--exchange=name", "Exchange to publish to (default \"\")") do |v|
          @exchange = v
        end
        @parser.on("-r pub-rate", "--rate=number", "Max publish rate (default 0)") do |v|
          @rate = v.to_i
        end
        @parser.on("-R consumer-rate", "--consumer-rate=number", "Max consume rate (default 0)") do |v|
          @consume_rate = v.to_i
        end
        @parser.on("-p", "--persistent", "Persistent messages (default false)") do
          @persistent = true
        end
        @parser.on("-j", "--json", "Output result as JSON") do
          @json_output = true
        end
        @parser.on("-z seconds", "--time=seconds", "Only run for X seconds") do |v|
          @timeout = Time::Span.new(seconds: v.to_i)
        end
        @parser.on("-q", "--quiet", "Quiet, only print the summary") do
          @quiet = true
        end
        @parser.on("-C messages", "--pmessages=messages", "Publish max X number of messages") do |v|
          @pmessages = v.to_i
        end
        @parser.on("-D messages", "--cmessages=messages", "Consume max X number of messages") do |v|
          @cmessages = v.to_i
        end
        @parser.on("--properties=JSON", "Properties added to published messages") do |v|
          @properties = JSON.parse(v).as_h
        end
        @parser.on("--random-bodies", "Each message body is random") do
          @random_bodies = true
        end
        @parser.on("--queue-pattern=PATTERN", "Queue name pattern (use % as placeholder, e.g. queue-%)") do |v|
          abort "Queue pattern must contain '%' placeholder" unless v.includes?("%")
          @queue_pattern = v
        end
        @parser.on("--queue-pattern-from=NUMBER", "Queue pattern start index (default 1)") do |v|
          from = v.to_i
          abort "Queue pattern from must be >= 1, got #{from}" if from < 1
          @queue_pattern_from = from
        end
        @parser.on("--queue-pattern-to=NUMBER", "Queue pattern end index (default 1)") do |v|
          to = v.to_i
          abort "Queue pattern to must be >= 1, got #{to}" if to < 1
          @queue_pattern_to = to
        end
        @parser.on("-l", "--measure-latency", "Measure and report end-to-end latency") do
          @measure_latency = true
        end
        @parser.on("--uri=URI", "HTTP API URI (e.g., http://guest:guest@localhost:15672 or https://localhost:15671)") do |v|
          @uri = URI.parse(v)
        end
        @parser.on("--vhost=NAME", "Virtual host (default /)") do |v|
          @vhost = v
        end
        @parser.on("--username=NAME", "Username (default guest)") do |v|
          @username = v
        end
        @parser.on("--password=PASS", "Password (default guest)") do |v|
          @password = v
        end
        @parser.on("--host=HOST", "HTTP API host (default localhost)") do |v|
          @host = v
        end
        @parser.on("--port=PORT", "HTTP API port (default 15672 for HTTP, 15671 for HTTPS)") do |v|
          @port = v.to_i
        end
        @parser.on("--insecure", "Bypass SSL certificate verification (for HTTPS)") do
          @insecure = true
        end
      end

      @pubs = Atomic(UInt64).new(0_u64)
      @consumes = Atomic(UInt64).new(0_u64)
      @stopped = false

      private def queue_names : Array(String)
        if pattern = @queue_pattern
          abort "Queue pattern from (#{@queue_pattern_from}) must be <= to (#{@queue_pattern_to})" if @queue_pattern_from > @queue_pattern_to
          queues = (@queue_pattern_from..@queue_pattern_to).map do |i|
            pattern.sub("%", i.to_s)
          end.to_a
          abort "Queue pattern generated empty queue list" if queues.empty?
          queues
        else
          [@queue]
        end
      end

      def run(args = ARGV)
        super(args)
        abort "Message size must be at least 8 bytes when measuring latency" if @measure_latency && @size < 8

        # Parse URI if provided
        if uri = @uri
          # Extract protocol
          case uri.scheme
          when "http"
            @tls = nil
          when "https"
            @tls = @insecure ? OpenSSL::SSL::Context::Client.insecure : OpenSSL::SSL::Context::Client.new
          else
            abort "Invalid URI scheme: #{uri.scheme}. Must be http or https"
          end

          # Extract host
          @host = uri.host || abort "URI must include a host"

          # Extract port (with defaults)
          @port = uri.port || (uri.scheme == "https" ? 15671 : 15672)

          # Extract credentials if present in URI
          if user = uri.user
            @username = user
          end
          if password = uri.password
            @password = password
          end
        else
          # No URI provided, use individual flags
          # Set up TLS if port suggests HTTPS (15671) or if explicitly using secure port
          if @port == 15671 && @tls.nil?
            @tls = @insecure ? OpenSSL::SSL::Context::Client.insecure : OpenSSL::SSL::Context::Client.new
          elsif @insecure
            # If --insecure is set but TLS wasn't configured, set it up
            @tls = OpenSSL::SSL::Context::Client.insecure
          end

          # Set default port if not specified
          @port ||= 15672
        end

        mt = Fiber::ExecutionContext::Parallel.new("Clients", maximum: System.cpu_count.to_i)
        queues = queue_names
        abort "No queues available for consumers" if queues.empty?

        unless @timeout.zero?
          spawn do
            sleep @timeout
            @stopped = true
          end
        end

        connected = WaitGroup.new
        done = WaitGroup.new

        # Ensure queues exist
        ensure_queues_exist(queues)

        connected.add(@consumers)
        done.add(@consumers)
        @consumers.times do |i|
          queue_name = queues[i % queues.size]
          mt.spawn { rerun_on_exception(done) { consume(connected, queue_name) } }
        end

        connected.wait # wait for all consumers to connect

        connected.add(@publishers)
        done.add(@publishers)
        @publishers.times do
          mt.spawn { rerun_on_exception(done) { pub(connected) } }
        end

        connected.wait # wait for all publishers to connect

        start = Time.instant
        Process.on_terminate do
          abort "Aborting" if @stopped
          @stopped = true
          summary(start)
          exit 0
        end

        spawn do
          done.wait
          @stopped = true
        end

        loop do
          break if @stopped
          pubs_last = @pubs.get(:relaxed)
          consumes_last = @consumes.get(:relaxed)
          report_start = Time.instant
          sleep 1.seconds
          report(report_start, pubs_last, consumes_last) unless @quiet
        end
        summary(start)
      end

      private def ensure_queues_exist(queues : Array(String))
        client = create_http_client
        vhost_encoded = URI.encode_path_segment(@vhost)

        queues.each do |queue_name|
          queue_encoded = URI.encode_path_segment(queue_name)
          path = "/api/queues/#{vhost_encoded}/#{queue_encoded}"
          body = {
            durable:     false,
            auto_delete: queue_name.empty?,
            arguments:   {} of String => String,
          }.to_json

          response = client.put(path, body: body)
          unless {201, 204}.includes?(response.status_code)
            abort "Failed to create queue #{queue_name}: #{response.status_code} #{response.body}"
          end
        end
      end

      private def create_http_client : ::HTTP::Client
        port = @port || 15672
        client = ::HTTP::Client.new(@host, port, tls: @tls)
        client.basic_auth(@username, @password)
        client
      end

      private def report(start : Time::Instant, pubs_last : UInt64, consumes_last : UInt64)
        elapsed = (Time.instant - start).total_seconds
        pubs = @pubs.get(:relaxed)
        consumes = @consumes.get(:relaxed)
        pub_rate = ((pubs - pubs_last) / elapsed).round.to_i64
        cons_rate = ((consumes - consumes_last) / elapsed).round.to_i64
        @io.print "Publish rate: #{pub_rate} msgs/s Consume rate: #{cons_rate} msgs/s"
        if @measure_latency
          stats = @latencies_mutex.synchronize do
            begin
              calculate_percentiles(@last_latencies)
            ensure
              @last_latencies.clear
            end
          end
          if stats
            @io.print " | Latency (ms) min/median/75th/95th/99th: #{stats[:min].round(3)}/#{stats[:median].round(3)}/#{stats[:p75].round(3)}/#{stats[:p95].round(3)}/#{stats[:p99].round(3)}"
          end
        end
        @io.puts
      end

      private def calculate_percentiles(latencies : Array(Float64))
        return if latencies.empty?
        latencies.unstable_sort!
        size = latencies.size - 1
        {
          min:    latencies.first,
          median: latencies[size * 50 // 100],
          p75:    latencies[size * 75 // 100],
          p95:    latencies[size * 95 // 100],
          p99:    latencies[size * 99 // 100],
        }
      end

      private def summary(start : Time::Instant)
        stop = Time.instant
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs.get(:relaxed) / elapsed).round.to_i64
        avg_consume = (@consumes.get(:relaxed) / elapsed).round.to_i64
        @io.puts
        latency_info = if @measure_latency
                         @latencies_mutex.synchronize do
                           {calculate_percentiles(@latencies), @latencies_count, @latencies.size}
                         end
                       end
        if @json_output
          JSON.build(@io) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "avg_pub_rate", avg_pub
              json.field "avg_consume_rate", avg_consume
              if latency_info
                latency_stats, latency_count, latency_sample_size = latency_info
                if latency_stats
                  json.field "latency_min_ms", latency_stats[:min].round(3)
                  json.field "latency_median_ms", latency_stats[:median].round(3)
                  json.field "latency_p75_ms", latency_stats[:p75].round(3)
                  json.field "latency_p95_ms", latency_stats[:p95].round(3)
                  json.field "latency_p99_ms", latency_stats[:p99].round(3)
                  json.field "latency_count", latency_count
                  json.field "latency_sample_size", latency_sample_size
                end
              end
            end
          end
          @io.puts
        else
          @io.puts "Summary:"
          @io.puts "Average publish rate: #{avg_pub} msgs/s"
          @io.puts "Average consume rate: #{avg_consume} msgs/s"
          if latency_info
            latency_stats, latency_count, latency_sample_size = latency_info
            if latency_stats
              if latency_count > latency_sample_size
                @io.puts "Latency (ms, n=#{latency_sample_size} sampled from #{latency_count}):"
              else
                @io.puts "Latency (ms, n=#{latency_count}):"
              end
              @io.puts "  min:    #{latency_stats[:min].round(3)}"
              @io.puts "  median: #{latency_stats[:median].round(3)}"
              @io.puts "  75th:   #{latency_stats[:p75].round(3)}"
              @io.puts "  95th:   #{latency_stats[:p95].round(3)}"
              @io.puts "  99th:   #{latency_stats[:p99].round(3)}"
            end
          end
        end
      end

      private def record_latency(body : Bytes)
        return if body.size < 8 # not enough data for timestamp

        timestamp_ns = IO::ByteFormat::LittleEndian.decode(Int64, body)
        latency_ms = ((Time.instant - BASE_INSTANT).total_nanoseconds - timestamp_ns) / 1_000_000.0
        @latencies_mutex.synchronize do
          @latencies_count += 1
          @last_latencies << latency_ms
          # Reservoir sampling: keep fixed-size sample
          if @latencies.size < LATENCY_RESERVOIR_SIZE
            @latencies << latency_ms
          else
            # Replace random element with probability RESERVOIR_SIZE / count
            # Standard reservoir sampling: pick random position in [0, count),
            # and if it's within reservoir size, replace that element
            j = @random.rand(@latencies_count)
            @latencies[j] = latency_ms if j < LATENCY_RESERVOIR_SIZE
          end
        end
      end

      private def verify_message_body(body : Bytes, expected : Bytes)
        if @measure_latency && body.size >= 8
          # Skip first 8 bytes (timestamp) when verifying
          raise "Invalid data" if body[8..] != expected[8..]
        else
          raise "Invalid data" if body != expected
        end
      end

      private def pub(connected)
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        props = @properties.dup
        props["delivery_mode"] = JSON::Any.new(2_i64) if @persistent

        client = create_http_client
        vhost_encoded = URI.encode_path_segment(@vhost)
        exchange_encoded = URI.encode_path_segment(@exchange)
        publish_path = "/api/exchanges/#{vhost_encoded}/#{exchange_encoded}/publish"

        wait_until_all_are_connected(connected)
        start = Time.instant
        pubs_this_second = 0
        queues = queue_names
        queue_idx = 0

        until @stopped
          if @measure_latency
            # Write timestamp at the beginning of the message
            IO::ByteFormat::LittleEndian.encode((Time.instant - BASE_INSTANT).total_nanoseconds.to_i64, data)
            # Fill the rest with random or pattern data
            if @random_bodies
              @random.random_bytes(data[8..])
            else
              (8...@size).each { |i| data[i] = ((i % 27 + 64)).to_u8 }
            end
          elsif @random_bodies
            @random.random_bytes(data)
          end

          # When using queue pattern, rotate through queues using queue name as routing key
          routing_key = if @queue_pattern
                          queues[queue_idx % queues.size]
                        else
                          @routing_key
                        end

          payload = Base64.strict_encode(data)
          body = {
            routing_key:      routing_key,
            payload:          payload,
            payload_encoding: "base64",
            properties:       props,
          }.to_json

          response = client.post(publish_path, body: body)
          unless response.status_code == 200
            @io.puts "Publish failed: #{response.status_code} #{response.body}"
            break
          end

          queue_idx += 1 if @queue_pattern
          pubs = @pubs.add(1, :relaxed) + 1
          break if pubs >= @pmessages > 0

          unless @rate.zero?
            pubs_this_second += 1
            if pubs_this_second >= @rate
              until_next_second = (start + 1.seconds) - Time.instant
              if until_next_second > Time::Span.zero
                sleep until_next_second
              end
              start = Time.instant
              pubs_this_second = 0
            end
          end
        end
      end

      private def consume(connected, queue_name : String)
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        client = create_http_client
        vhost_encoded = URI.encode_path_segment(@vhost)
        queue_encoded = URI.encode_path_segment(queue_name)
        get_path = "/api/queues/#{vhost_encoded}/#{queue_encoded}/get"

        wait_until_all_are_connected(connected)
        rate_limiter = RateLimiter.new(@consume_rate)

        until @stopped
          get_body = {
            count:    1,
            ack_mode: "get",
            requeue: false,
            encoding: "base64",
          }.to_json

          response = client.post(get_path, body: get_body)
          unless response.status_code == 200
            # No messages available, sleep briefly
            sleep 1.millisecond
            next
          end

          messages = JSON.parse(response.body).as_a
          messages.each do |msg|
            consumes = @consumes.add(1, :relaxed) + 1
            payload_str = msg["payload"].as_s
            payload = Base64.decode(payload_str)
            body_bytes = Bytes.new(payload.size) { |i| payload[i] }

            record_latency(body_bytes) if @measure_latency
            verify_message_body(body_bytes, data) if @verify

            if @stopped || consumes >= @cmessages > 0
              return
            end
            rate_limiter.limit(consumes)
          end
        end
      end

      private def rerun_on_exception(done, &)
        loop do
          break yield
        rescue ex : IO::Error | Socket::Error | Exception
          @io.puts ex.message
          sleep 1.seconds
        end
      ensure
        done.done
      end

      # Announce that the client is connected
      # and then wait for all other clients to be connected too
      private def wait_until_all_are_connected(connected)
        connected.done
        connected.wait
      rescue
        # when we reconnect a broker the waitgroup will have a negative counter
      end

      private class RateLimiter
        def initialize(@rate : Int32)
          @consumes_this_second = 0
          @start = Time.instant
          @total_consumes = 0_u64
        end

        def limit(consume_count : UInt64? = nil)
          if @rate.zero?
            # When no rate limiting, yield periodically to avoid blocking other fibers
            count = consume_count || @total_consumes
            Fiber.yield if count % (128*1024) == 0
            return
          end

          @consumes_this_second += 1
          @total_consumes += 1
          if @consumes_this_second >= @rate
            until_next_second = (@start + 1.seconds) - Time.instant
            sleep until_next_second if until_next_second > Time::Span.zero
            @start = Time.instant
            @consumes_this_second = 0
          end
        end
      end
    end
  end
end
