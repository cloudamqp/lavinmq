require "mqtt-client"
require "json"
require "wait_group"
require "../perf"

module LavinMQPerf
  module MQTT
    class Throughput < Perf
      @publishers = 1
      @consumers = 1
      @size = 16
      @verify = false
      @topic = "perf-test"
      @qos = 0
      @rate = 0
      @consume_rate = 0
      @quiet = false
      @json_output = false
      @timeout = Time::Span.zero
      @pmessages = 0
      @cmessages = 0
      @random_bodies = false
      @retain = false
      @clean_session = false

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
        @parser.on("-q qos", "--qos=level", "QoS level (0, 1, or 2)") do |v|
          @qos = v.to_i
        end
        @parser.on("-t topic", "--topic=name", "Topic name (default perf-test)") do |v|
          @topic = v
        end
        @parser.on("-r pub-rate", "--rate=number", "Max publish rate (default 0)") do |v|
          @rate = v.to_i
        end
        @parser.on("-R consumer-rate", "--consumer-rate=number", "Max consume rate (default 0)") do |v|
          @consume_rate = v.to_i
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
        @parser.on("--random-bodies", "Each message body is random") do
          @random_bodies = true
        end
        @parser.on("--retain", "Set retain flag on published messages") do
          @retain = true
        end
        @parser.on("--clean-session", "Use clean session") do
          @clean_session = true
        end
      end

      @pubs = 0_u64
      @consumes = 0_u64
      @stopped = false
      @start_time = Time.monotonic

      def run
        super

        done = WaitGroup.new(@consumers + @publishers)

        @consumers.times do |i|
          spawn { reconnect_on_disconnect(done) { consume(i) } }
        end

        sleep 1.seconds # Give consumers time to connect

        @publishers.times do |i|
          spawn { reconnect_on_disconnect(done) { pub(i) } }
        end

        if @timeout != Time::Span.zero
          spawn do
            sleep @timeout
            @stopped = true
          end
        end

        Fiber.yield # wait for all clients to connect
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
          pubs_last = @pubs
          consumes_last = @consumes
          sleep 1.seconds
          unless @quiet
            puts "Publish rate: #{@pubs - pubs_last} msgs/s Consume rate: #{@consumes - consumes_last} msgs/s"
          end
        end
        summary(start)
      end

      private def summary(start : Time::Span)
        stop = Time.monotonic
        elapsed = (stop - start).total_seconds
        avg_pub = (@pubs / elapsed).round(1)
        avg_consume = (@consumes / elapsed).round(1)
        puts
        if @json_output
          JSON.build(STDOUT) do |json|
            json.object do
              json.field "elapsed_seconds", elapsed
              json.field "avg_pub_rate", avg_pub
              json.field "avg_consume_rate", avg_consume
              json.field "total_published", @pubs
              json.field "total_consumed", @consumes
            end
          end
          puts
        else
          puts "Summary:"
          puts "Average publish rate: #{avg_pub} msgs/s"
          puts "Average consume rate: #{avg_consume} msgs/s"
          puts "Total published: #{@pubs}"
          puts "Total consumed: #{@consumes}"
        end
      end

      private def pub(id : Int32)
        client = ::MQTT::Client.new("localhost", 1883,
          user: "guest",
          password: "guest",
          client_id: "publisher-#{id}",
          clean_session: @clean_session)

        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Random::DEFAULT.random_bytes(data) if @random_bodies
        Fiber.yield

        start = Time.monotonic
        pubs_this_second = 0
        until @stopped
          client.publish(@topic, data, qos: @qos, retain: @retain)
          @pubs += 1
          break if @pubs == @pmessages

          start, pubs_this_second = rate_limit(pubs_this_second, @rate, start)
        end
        client.disconnect
      end

      private def consume(id : Int32)
        client = ::MQTT::Client.new("localhost", 1883,
          user: "guest",
          password: "guest",
          client_id: "consumer-#{id}",
          clean_session: @clean_session)

        data = Bytes.new(@size) { |i| ((i % 27 + 64)).to_u8 }
        Random::DEFAULT.random_bytes(data) if @random_bodies
        Fiber.yield

        start = Time.monotonic
        consumes_this_second = 0
        done_channel = Channel(Nil).new

        client.on_message do |msg|
          @consumes += 1
          if @verify
            received_data = msg.body.to_slice
            if @random_bodies
              raise "Invalid data size: #{received_data.size} != #{@size}" if received_data.size != @size
            else
              raise "Invalid data: #{received_data} != #{data}" if received_data != data
            end
          end

          if @consumes == @cmessages
            done_channel.send(nil)
            next
          end

          start, consumes_this_second = rate_limit(consumes_this_second, @consume_rate, start)
        end

        client.subscribe(@topic, @qos)
        done_channel.receive
        client.disconnect
      end

      private def rate_limit(current_count : Int32, rate : Int32, start_time : Time::Span) : {Time::Span, Int32}
        return {start_time, current_count} if rate.zero?

        current_count += 1
        if current_count >= rate
          until_next_second = (start_time + 1.seconds) - Time.monotonic
          if until_next_second > Time::Span.zero
            sleep until_next_second
          end
          return {Time.monotonic, 0}
        end

        {start_time, current_count}
      end

      private def reconnect_on_disconnect(done, &)
        loop do
          break yield
        rescue ex : Exception
          puts ex.message
          sleep 1.seconds
        end
      ensure
        done.done
      end
    end
  end
end
