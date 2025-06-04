require "amqp-client"
require "json"
require "wait_group"
require "../perf"

module LavinMQPerf
  module AMQP
    class Throughput < Perf
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
      end

      @pubs = Atomic(UInt64).new(0_u64)
      @consumes = Atomic(UInt64).new(0_u64)
      @stopped = false

      def run
        super

        mt = Fiber::ExecutionContext::MultiThreaded.new("Clients", maximum: System.cpu_count.to_i)
        connected = WaitGroup.new(@consumers + @publishers)
        done = WaitGroup.new(@consumers + @publishers)
        @consumers.times do
          if @poll
            mt.spawn { rerun_on_exception(done) { poll_consume(connected) } }
          else
            mt.spawn { rerun_on_exception(done) { consume(connected) } }
          end
        end

        @publishers.times do
          mt.spawn { rerun_on_exception(done) { pub(connected) } }
        end

        if @timeout != Time::Span.zero
          spawn do
            sleep @timeout
            @stopped = true
          end
        end

        connected.wait # wait for all clients to connect
        start = Time.monotonic
        Signal::INT.trap do
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
          pubs_last = @pubs.get
          consumes_last = @consumes.get
          sleep 1.seconds
          unless @quiet
            puts "Publish rate: #{@pubs.get - pubs_last} msgs/s Consume rate: #{@consumes.get - consumes_last} msgs/s"
          end
        end
        summary(start)
      end

      private def summary(start : Time::Span)
        stop = Time.monotonic
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs.get / elapsed).round(1)
        avg_consume = (@consumes.get / elapsed).round(1)
        puts
        if @json_output
          JSON.build(STDOUT) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "avg_pub_rate", avg_pub
              json.field "avg_consume_rate", avg_consume
            end
          end
          puts
        else
          puts "Summary:"
          puts "Average publish rate: #{avg_pub} msgs/s"
          puts "Average consume rate: #{avg_consume} msgs/s"
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
          until @stopped
            Random::DEFAULT.random_bytes(data) if @random_bodies
            if @max_unconfirm > 0
              unconfirmed.send nil
              ch.basic_publish(data, @exchange, @routing_key, props: props) do
                unconfirmed.receive
              end
            else
              ch.basic_publish(data, @exchange, @routing_key, props: props)
            end
            pubs = @pubs.add(1)
            ch.tx_commit if @pub_in_transaction > 0 && (pubs % @pub_in_transaction) == 0
            break if (pubs + 1) == @pmessages
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

      private def consume(connected) # ameba:disable Metrics/CyclomaticComplexity
        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          q = begin
            ch.queue @queue, durable: !@queue.empty?, auto_delete: @queue.empty?, args: @queue_args
          rescue
            ch = a.channel
            ch.queue(@queue, passive: true)
          end
          ch.tx_select if @ack_in_transaction > 0
          if prefetch = @prefetch
            ch.prefetch prefetch
          end
          q.bind(@exchange, @routing_key) unless @exchange.empty?
          wait_until_all_are_connected(connected)
          consumes_this_second = 0
          start = Time.monotonic
          q.subscribe(tag: "c", no_ack: @ack.zero?, block: true, args: @consumer_args) do |m|
            consumes = @consumes.add(1)
            raise "Invalid data: #{m.body_io.to_slice}" if @verify && m.body_io.to_slice != data
            m.ack(multiple: true) if @ack > 0 && (consumes + 1) % @ack == 0
            ch.tx_commit if @ack_in_transaction > 0 && ((consumes + 1) % @ack_in_transaction) == 0
            if @stopped || (consumes + 1) == @cmessages
              ch.close
            end
            if @consume_rate.zero?
              Fiber.yield if consumes % 128*1024 == 0
            else
              consumes_this_second += 1
              if consumes_this_second >= @consume_rate
                until_next_second = (start + 1.seconds) - Time.monotonic
                if until_next_second > Time::Span.zero
                  sleep until_next_second
                end
                start = Time.monotonic
                consumes_this_second = 0
              end
            end
          end
        end
      end

      private def poll_consume(connected)
        ::AMQP::Client.start(@uri) do |a|
          ch = a.channel
          q = begin
            ch.queue @queue
          rescue
            ch = a.channel
            ch.queue(@queue, passive: true)
          end
          if prefetch = @prefetch
            ch.prefetch prefetch
          end
          q.bind(@exchange, @routing_key) unless @exchange.empty?
          wait_until_all_are_connected(connected)
          consumes_this_second = 0
          start = Time.monotonic
          loop do
            if msg = q.get(no_ack: @ack.zero?)
              consumes = @consumes.add(1)
              msg.ack(multiple: true) if @ack > 0 && (consumes + 1) % @ack == 0
              break if @stopped || (consumes + 1) == @cmessages
            end
            unless @consume_rate.zero?
              consumes_this_second += 1
              if consumes_this_second >= @consume_rate
                until_next_second = (start + 1.seconds) - Time.monotonic
                if until_next_second > Time::Span.zero
                  sleep until_next_second
                end
                start = Time.monotonic
                consumes_this_second = 0
              end
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
    end
  end
end
