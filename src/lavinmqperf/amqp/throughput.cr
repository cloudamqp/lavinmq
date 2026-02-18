require "amqp-client"
require "json"
require "wait_group"
require "../perf"

module LavinMQPerf
  module AMQP
    class Throughput < Perf
      LATENCY_RESERVOIR_SIZE = 128 * 1024

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
      @max_unconfirm = 0
      @persistent = false
      @prefetch : UInt16? = nil
      @quiet = false
      @poll = false
      @json_output = false
      @timeout = Time::Span.zero
      @pmessages = 0
      @cmessages = 0
      @queue_args = ::AMQP::Client::Arguments.new
      @consumer_args = ::AMQP::Client::Arguments.new
      @properties = AMQ::Protocol::Properties.new
      @pub_in_transaction = 0
      @ack_in_transaction = 0
      @random_bodies = false
      @queue_pattern : String? = nil
      @queue_pattern_from = 1
      @queue_pattern_to = 1
      @measure_latency = false
      @latencies = Array(Float64).new(LATENCY_RESERVOIR_SIZE)
      @latencies_mutex = Mutex.new
      @last_latencies = Array(Float64).new
      @latencies_count : UInt64 = 0

      def initialize
        super
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
        @parser.on("-c max-unconfirmed", "--confirm max-unconfirmed", "Confirm publishes every X messages") do |v|
          @max_unconfirm = v.to_i
        end
        @parser.on("-t msgs-in-transaction", "--transaction max-uncommited-messages", "Publish messages in transactions") do |v|
          @pub_in_transaction = v.to_i
        end
        @parser.on("-T acks-in-transaction", "--transaction max-uncommited-acknowledgements", "Ack messages in transactions") do |v|
          @ack_in_transaction = v.to_i
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
        @parser.on("-P", "--prefetch=number", "Number of messages to prefetch)") do |v|
          @prefetch = v.to_u16
        end
        @parser.on("-g", "--poll", "Poll with basic_get instead of consuming") do
          @poll = true
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
        @parser.on("--queue-args=JSON", "Queue arguments as a JSON string") do |v|
          @queue_args = ::AMQP::Client::Arguments.new(JSON.parse(v).as_h)
        end
        @parser.on("--consumer-args=JSON", "Consumer arguments as a JSON string") do |v|
          @consumer_args = ::AMQP::Client::Arguments.new(JSON.parse(v).as_h)
        end
        @parser.on("--properties=JSON", "Properties added to published messages") do |v|
          @properties = AMQ::Protocol::Properties.from_json(JSON.parse(v))
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

      def run
        super

        abort "Message size must be at least 8 bytes when measuring latency" if @measure_latency && @size < 8

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

        connected.add(@consumers)
        done.add(@consumers)
        @consumers.times do |i|
          queue_name = queues[i % queues.size]
          if @poll
            mt.spawn { rerun_on_exception(done) { poll_consume(connected, queue_name) } }
          else
            mt.spawn { rerun_on_exception(done) { consume(connected, queue_name) } }
          end
        end

        connected.wait # wait for all consumers to connect

        connected.add(@publishers)
        done.add(@publishers)
        @publishers.times do
          mt.spawn { rerun_on_exception(done) { pub(connected) } }
        end

        connected.wait # wait for all publishers to connect

        start = Time.monotonic
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
          report_start = Time.monotonic
          sleep 1.seconds
          report(report_start, pubs_last, consumes_last) unless @quiet
        end
        summary(start)
      end

      private def report(start : Time::Span, pubs_last : UInt64, consumes_last : UInt64)
        elapsed = (Time.monotonic - start).total_seconds
        pubs = @pubs.get(:relaxed)
        consumes = @consumes.get(:relaxed)
        pub_rate = ((pubs - pubs_last) / elapsed).round.to_i64
        cons_rate = ((consumes - consumes_last) / elapsed).round.to_i64
        print "Publish rate: #{pub_rate} msgs/s Consume rate: #{cons_rate} msgs/s"
        if @measure_latency
          stats = @latencies_mutex.synchronize do
            begin
              calculate_percentiles(@last_latencies)
            ensure
              @last_latencies.clear
            end
          end
          if stats
            print " | Latency (ms) min/median/75th/95th/99th: #{stats[:min].round(3)}/#{stats[:median].round(3)}/#{stats[:p75].round(3)}/#{stats[:p95].round(3)}/#{stats[:p99].round(3)}"
          end
        end
        puts
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

      private def summary(start : Time::Span)
        stop = Time.monotonic
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs.get(:relaxed) / elapsed).round.to_i64
        avg_consume = (@consumes.get(:relaxed) / elapsed).round.to_i64
        puts
        latency_info = if @measure_latency
                         @latencies_mutex.synchronize do
                           {calculate_percentiles(@latencies), @latencies_count, @latencies.size}
                         end
                       end
        if @json_output
          JSON.build(STDOUT) do |json|
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
          puts
        else
          puts "Summary:"
          puts "Average publish rate: #{avg_pub} msgs/s"
          puts "Average consume rate: #{avg_consume} msgs/s"
          if latency_info
            latency_stats, latency_count, latency_sample_size = latency_info
            if latency_stats
              if latency_count > latency_sample_size
                puts "Latency (ms, n=#{latency_sample_size} sampled from #{latency_count}):"
              else
                puts "Latency (ms, n=#{latency_count}):"
              end
              puts "  min:    #{latency_stats[:min].round(3)}"
              puts "  median: #{latency_stats[:median].round(3)}"
              puts "  75th:   #{latency_stats[:p75].round(3)}"
              puts "  95th:   #{latency_stats[:p95].round(3)}"
              puts "  99th:   #{latency_stats[:p99].round(3)}"
            end
          end
        end
      end

      private def record_latency(body : Bytes)
        return if body.size < 8 # not enough data for timestamp

        timestamp_ns = IO::ByteFormat::LittleEndian.decode(Int64, body)
        latency_ms = (Time.monotonic.total_nanoseconds - timestamp_ns) / 1_000_000.0
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
            j = Random::DEFAULT.rand(@latencies_count)
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

      private def pub(connected) # ameba:disable Metrics/CyclomaticComplexity
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        props = @properties
        props.delivery_mode = 2u8 if @persistent
        unconfirmed = ::Channel(Nil).new(@max_unconfirm)
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          ch.tx_select if @pub_in_transaction > 0
          ch.confirm_select if @max_unconfirm > 0
          wait_until_all_are_connected(connected)
          start = Time.monotonic
          pubs_this_second = 0
          queues = queue_names
          queue_idx = 0
          local_pubs = 0_u64
          until @stopped
            if @measure_latency
              # Write timestamp at the beginning of the message
              IO::ByteFormat::LittleEndian.encode(Time.monotonic.total_nanoseconds.to_i64, data)
              # Fill the rest with random or pattern data
              if @random_bodies
                Random::DEFAULT.random_bytes(data[8..])
              else
                (8...@size).each { |i| data[i] = ((i % 27 + 64)).to_u8 }
              end
            elsif @random_bodies
              Random::DEFAULT.random_bytes(data)
            end
            # When using queue pattern, rotate through queues using queue name as routing key
            routing_key = if @queue_pattern
                            queues[queue_idx % queues.size]
                          else
                            @routing_key
                          end
            if @max_unconfirm > 0
              unconfirmed.send nil
              ch.basic_publish(data, @exchange, routing_key, props: props) do
                unconfirmed.receive
              end
            else
              ch.basic_publish(data, @exchange, routing_key, props: props)
            end
            queue_idx += 1 if @queue_pattern
            pubs = @pubs.add(1, :relaxed) + 1
            local_pubs &+= 1
            ch.tx_commit if @pub_in_transaction > 0 && (local_pubs % @pub_in_transaction) == 0
            break if pubs >= @pmessages > 0
            Fiber.yield if @rate.zero? && local_pubs % (128*1024) == 0
            unless @rate.zero?
              pubs_this_second += 1
              if pubs_this_second >= @rate
                until_next_second = (start + 1.seconds) - Time.monotonic
                if until_next_second > Time::Span.zero
                  sleep until_next_second
                end
                start = Time.monotonic
                pubs_this_second = 0
              end
            end
          end
        end
      end

      private def consume(connected, queue_name : String)
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          q = begin
            ch.queue queue_name, durable: !queue_name.empty?, auto_delete: queue_name.empty?, args: @queue_args
          rescue
            ch = a.channel
            ch.queue(queue_name, passive: true)
          end
          q.purge if @measure_latency # Can't measure latency with existing messages
          ch.tx_select if @ack_in_transaction > 0
          if prefetch = @prefetch
            ch.prefetch prefetch
          end
          q.bind(@exchange, @routing_key) unless @exchange.empty?
          wait_until_all_are_connected(connected)
          rate_limiter = RateLimiter.new(@consume_rate)
          local_consumes = 0_u64
          q.subscribe(tag: "c", no_ack: @ack.zero?, block: true, args: @consumer_args) do |m|
            local_consumes &+= 1
            handle_consumed_message(ch, m, data, rate_limiter, local_consumes)
          end
        end
      end

      private def handle_consumed_message(ch, m, data, rate_limiter, local_consumes : UInt64)
        consumes = @consumes.add(1, :relaxed) + 1
        body = m.body_io.to_slice
        record_latency(body) if @measure_latency
        verify_message_body(body, data) if @verify
        m.ack(multiple: true) if @ack > 0 && local_consumes % @ack == 0
        ch.tx_commit if @ack_in_transaction > 0 && (local_consumes % @ack_in_transaction) == 0
        if @stopped || consumes >= @cmessages > 0
          ch.close
        end
        rate_limiter.limit
      end

      private def poll_consume(connected, queue_name : String)
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          q = begin
            ch.queue queue_name
          rescue
            ch = a.channel
            ch.queue(queue_name, passive: true)
          end
          q.purge if @measure_latency # Can't measure latency with existing messages
          if prefetch = @prefetch
            ch.prefetch prefetch
          end
          q.bind(@exchange, @routing_key) unless @exchange.empty?
          wait_until_all_are_connected(connected)
          rate_limiter = RateLimiter.new(@consume_rate)
          local_consumes = 0_u64
          loop do
            if msg = q.get(no_ack: @ack.zero?)
              consumes = @consumes.add(1, :relaxed) + 1
              local_consumes &+= 1
              record_latency(msg.body_io.to_slice) if @measure_latency
              msg.ack(multiple: true) if @ack > 0 && local_consumes % @ack == 0
              break if @stopped || consumes >= @cmessages > 0
              rate_limiter.limit
            end
          end
        end
      end

      private def rerun_on_exception(done, &)
        loop do
          break yield
        rescue ex : ::AMQP::Client::Error | IO::Error
          puts ex.message
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
          @start = Time.monotonic
          @total_consumes = 0_u64
        end

        def limit
          @total_consumes &+= 1
          if @rate.zero?
            # When no rate limiting, yield periodically to avoid blocking other fibers
            Fiber.yield if @total_consumes % (128*1024) == 0
            return
          end

          @consumes_this_second += 1
          if @consumes_this_second >= @rate
            until_next_second = (@start + 1.seconds) - Time.monotonic
            sleep until_next_second if until_next_second > Time::Span.zero
            @start = Time.monotonic
            @consumes_this_second = 0
          end
        end
      end
    end
  end
end
