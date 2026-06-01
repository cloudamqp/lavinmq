require "amqp-client"
require "json"
require "wait_group"
require "../perf"

module LavinMQPerf
  module AMQP
    # Benchmark queue with automatic retry (x-retry-delay).
    # Each consumed message is rejected (requeue=true) until @retry_count
    # retries are observed, then acked. Measures publish rate, retry rate,
    # final ack rate, observed-vs-expected delay deviation, and end-to-end
    # latency from publish to final ack.
    class RetryBench < Perf
      BASE_INSTANT = Time.instant

      @publishers = 1
      @consumers = 1
      @queue = "retry-bench"
      @size = 24
      @retry_delay = 100
      @retry_multiplier = 1
      @retry_max_delay = 60000
      @retry_count = 3
      @delivery_limit : Int32? = nil
      @persistent = false
      @prefetch : UInt16 = 0_u16
      @rate = 0
      @pmessages = 0
      @timeout = Time::Span.zero
      @json_output = false
      @quiet = false

      def initialize(io : IO = STDOUT)
        super(io)
        @parser.banner = "Usage: #{PROGRAM_NAME} amqp retry-bench [options]"
        @parser.on("-x publishers", "--publishers=N", "Number of publishers (default 1)") { |v| @publishers = v.to_i }
        @parser.on("-y consumers", "--consumers=N", "Number of consumers (default 1)") { |v| @consumers = v.to_i }
        @parser.on("-u queue", "--queue=NAME", "Queue name (default retry-bench)") { |v| @queue = v }
        @parser.on("-s msgsize", "--size=BYTES", "Message size in bytes, min 16 (default 24)") { |v| @size = v.to_i }
        @parser.on("--retry-delay=MS", "Initial retry delay ms (default 100)") { |v| @retry_delay = v.to_i }
        @parser.on("--retry-multiplier=N", "Retry delay multiplier (default 1)") { |v| @retry_multiplier = v.to_i }
        @parser.on("--retry-max-delay=MS", "Max retry delay ms (default 60000)") { |v| @retry_max_delay = v.to_i }
        @parser.on("--retry-count=N", "Rejects per message before final ack (default 3)") { |v| @retry_count = v.to_i }
        @parser.on("--delivery-limit=N", "Override x-delivery-limit (default retry-count+1)") { |v| @delivery_limit = v.to_i }
        @parser.on("-p", "--persistent", "Persistent messages") { @persistent = true }
        @parser.on("-P prefetch", "--prefetch=N", "Consumer prefetch (default 0)") { |v| @prefetch = v.to_u16 }
        @parser.on("-r rate", "--rate=N", "Max publish rate per publisher (default 0=unlimited)") { |v| @rate = v.to_i }
        @parser.on("-C messages", "--pmessages=N", "Publish max N messages (default 0=unlimited)") { |v| @pmessages = v.to_i }
        @parser.on("-z seconds", "--time=SECONDS", "Run for SECONDS then stop") { |v| @timeout = Time::Span.new(seconds: v.to_i) }
        @parser.on("-j", "--json", "Output summary as JSON") { @json_output = true }
        @parser.on("-q", "--quiet", "Only print final summary") { @quiet = true }
      end

      @pubs = Atomic(UInt64).new(0_u64)
      @retries = Atomic(UInt64).new(0_u64)
      @acks = Atomic(UInt64).new(0_u64)
      @stopped = false
      @seq = Atomic(UInt64).new(0_u64)

      @delay_dev_mutex = Mutex.new
      @delay_deviations_ms = Array(Float64).new       # since last report
      @delay_deviations_total_ms = Array(Float64).new # cumulative for summary
      @e2e_mutex = Mutex.new
      @e2e_latencies_ms = Array(Float64).new

      def run(args = ARGV)
        super(args)
        abort "Message size must be >= 16 bytes" if @size < 16
        abort "Retry count must be >= 0" if @retry_count < 0

        mt = Fiber::ExecutionContext::Parallel.new("Clients", maximum: System.cpu_count.to_i)

        declare_queue

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
        @consumers.times do
          mt.spawn { rerun_on_exception(done) { consume(connected) } }
        end
        connected.wait

        connected.add(@publishers)
        done.add(@publishers)
        @publishers.times do
          mt.spawn { rerun_on_exception(done) { pub(connected) } }
        end
        connected.wait

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
          retries_last = @retries.get(:relaxed)
          acks_last = @acks.get(:relaxed)
          report_start = Time.instant
          sleep 1.seconds
          report(report_start, pubs_last, retries_last, acks_last) unless @quiet
        end
        summary(start)
      end

      private def declare_queue
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          args = ::AMQP::Client::Arguments.new
          args["x-retry-delay"] = @retry_delay
          args["x-retry-delay-multiplier"] = @retry_multiplier if @retry_multiplier != 1
          args["x-retry-max-delay"] = @retry_max_delay if @retry_max_delay != 60000
          args["x-delivery-limit"] = @delivery_limit || (@retry_count + 1)
          ch.queue(@queue, durable: @persistent, args: args)
        end
      end

      private def report(start : Time::Instant, pubs_last : UInt64, retries_last : UInt64, acks_last : UInt64)
        elapsed = (Time.instant - start).total_seconds
        pubs = @pubs.get(:relaxed)
        retries = @retries.get(:relaxed)
        acks = @acks.get(:relaxed)
        pub_rate = ((pubs - pubs_last) / elapsed).round.to_i64
        retry_rate = ((retries - retries_last) / elapsed).round.to_i64
        ack_rate = ((acks - acks_last) / elapsed).round.to_i64
        dev = @delay_dev_mutex.synchronize { stats = percentiles(@delay_deviations_ms.dup); @delay_deviations_ms.clear; stats }
        @io.print "Publish: #{pub_rate}/s  Retry: #{retry_rate}/s  Ack: #{ack_rate}/s"
        if dev
          @io.print "  Delay deviation ms p50/p95/p99: #{fmt(dev[:p50])}/#{fmt(dev[:p95])}/#{fmt(dev[:p99])}"
        end
        @io.puts
      end

      private def fmt(v : Float64) : String
        v.round(2).to_s
      end

      private def percentiles(values : Array(Float64))
        return if values.empty?
        values.unstable_sort!
        size = values.size - 1
        {
          min: values.first,
          p50: values[size * 50 // 100],
          p95: values[size * 95 // 100],
          p99: values[size * 99 // 100],
          max: values.last,
        }
      end

      private def summary(start : Time::Instant)
        elapsed = ((Time.instant) - start).total_seconds
        avg_pub = (@pubs.get(:relaxed) / elapsed).round.to_i64
        avg_retry = (@retries.get(:relaxed) / elapsed).round.to_i64
        avg_ack = (@acks.get(:relaxed) / elapsed).round.to_i64
        dev = @delay_dev_mutex.synchronize { percentiles(@delay_deviations_total_ms.dup) }
        e2e = @e2e_mutex.synchronize { percentiles(@e2e_latencies_ms.dup) }
        @io.puts
        if @json_output
          JSON.build(@io) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "publishes_total", @pubs.get(:relaxed)
              json.field "retries_total", @retries.get(:relaxed)
              json.field "acks_total", @acks.get(:relaxed)
              json.field "avg_publish_per_sec", avg_pub
              json.field "avg_retry_per_sec", avg_retry
              json.field "avg_ack_per_sec", avg_ack
              if dev
                json.field "delay_deviation_ms" do
                  json.object do
                    json.field "min", dev[:min].round(3)
                    json.field "p50", dev[:p50].round(3)
                    json.field "p95", dev[:p95].round(3)
                    json.field "p99", dev[:p99].round(3)
                    json.field "max", dev[:max].round(3)
                  end
                end
              end
              if e2e
                json.field "end_to_end_latency_ms" do
                  json.object do
                    json.field "min", e2e[:min].round(3)
                    json.field "p50", e2e[:p50].round(3)
                    json.field "p95", e2e[:p95].round(3)
                    json.field "p99", e2e[:p99].round(3)
                    json.field "max", e2e[:max].round(3)
                  end
                end
              end
            end
          end
          @io.puts
        else
          @io.puts "Summary:"
          @io.puts "  Publishes: #{@pubs.get(:relaxed)} (#{avg_pub}/s avg)"
          @io.puts "  Retries:   #{@retries.get(:relaxed)} (#{avg_retry}/s avg)"
          @io.puts "  Acks:      #{@acks.get(:relaxed)} (#{avg_ack}/s avg)"
          if dev
            @io.puts "  Delay deviation (observed - expected, ms):"
            @io.puts "    min/p50/p95/p99/max: #{fmt(dev[:min])}/#{fmt(dev[:p50])}/#{fmt(dev[:p95])}/#{fmt(dev[:p99])}/#{fmt(dev[:max])}"
          end
          if e2e
            @io.puts "  End-to-end latency (publish→ack, ms):"
            @io.puts "    min/p50/p95/p99/max: #{fmt(e2e[:min])}/#{fmt(e2e[:p50])}/#{fmt(e2e[:p95])}/#{fmt(e2e[:p99])}/#{fmt(e2e[:max])}"
          end
        end
      end

      private def expected_delay_ms(attempt : Int32) : Int32
        # attempt is 1-based: first redelivery (after first reject) is attempt 1
        d = @retry_delay
        (attempt - 1).times { d *= @retry_multiplier }
        Math.min(d, @retry_max_delay)
      end

      private def pub(connected)
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        props = ::AMQ::Protocol::Properties.new
        props.delivery_mode = 2u8 if @persistent
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          wait_until_all_are_connected(connected)
          rate_start = Time.instant
          this_sec = 0
          loop do
            break if @stopped
            now_ns = (Time.instant - BASE_INSTANT).total_nanoseconds.to_i64
            seq = @seq.add(1, :relaxed) + 1
            IO::ByteFormat::LittleEndian.encode(now_ns, data)
            IO::ByteFormat::LittleEndian.encode(seq.to_i64, data + 8)
            ch.basic_publish(data, "", @queue, props: props)
            pubs = @pubs.add(1, :relaxed) + 1
            break if @pmessages > 0 && pubs >= @pmessages
            unless @rate.zero?
              this_sec += 1
              if this_sec >= @rate
                left = (rate_start + 1.seconds) - Time.instant
                sleep left if left > Time::Span.zero
                rate_start = Time.instant
                this_sec = 0
              end
            end
          end
        end
      end

      private def consume(connected)
        # seq_id => last_delivery_ns
        last_seen = Hash(Int64, Int64).new
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          ch.prefetch @prefetch if @prefetch > 0
          q = ch.queue(@queue, passive: true)
          wait_until_all_are_connected(connected)
          q.subscribe(no_ack: false, block: true) do |m|
            handle(ch, m, last_seen)
            ch.close if @stopped
          end
        end
      end

      private def handle(ch, m, last_seen : Hash(Int64, Int64))
        body = m.body_io.to_slice
        return m.ack if body.size < 16
        publish_ns = IO::ByteFormat::LittleEndian.decode(Int64, body)
        seq = IO::ByteFormat::LittleEndian.decode(Int64, body + 8)
        now_ns = (Time.instant - BASE_INSTANT).total_nanoseconds.to_i64

        delivery_count = (m.properties.headers.try(&.["x-delivery-count"]?).try(&.as?(Int)) || 0).to_i32

        if delivery_count > 0
          if prev_ns = last_seen[seq]?
            observed_ms = (now_ns - prev_ns) / 1_000_000.0
            expected_ms = expected_delay_ms(delivery_count).to_f64
            @delay_dev_mutex.synchronize do
              dev = observed_ms - expected_ms
              @delay_deviations_ms << dev
              @delay_deviations_total_ms << dev
            end
          end
        end

        if delivery_count >= @retry_count
          m.ack
          @acks.add(1, :relaxed)
          last_seen.delete(seq)
          e2e_ms = (now_ns - publish_ns) / 1_000_000.0
          @e2e_mutex.synchronize { @e2e_latencies_ms << e2e_ms }
        else
          last_seen[seq] = now_ns
          m.reject(requeue: true)
          @retries.add(1, :relaxed)
        end
      end

      private def rerun_on_exception(done, &)
        loop do
          break yield
        rescue ex : ::AMQP::Client::Error | IO::Error
          @io.puts ex.message
          sleep 1.seconds
        end
      ensure
        done.done
      end

      private def wait_until_all_are_connected(connected)
        connected.done
        connected.wait
      rescue
      end
    end
  end
end
