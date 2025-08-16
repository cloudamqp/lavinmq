require "amqp-client"
require "../perf"

module LavinMQPerf
  module AMQP
    class QueueCount < Perf
      @queues = 100
      @durable = false
      @exclusive = true
      @no_wait = false
      @args = ::AMQP::Client::Arguments.new

      def initialize
        super
        @parser.on("-q queues", "--queues=number", "Number of queues (default 100)") do |v|
          @queues = v.to_i
        end
        @parser.on("-D", "--durable", "If the queues are durable or not") do
          @durable = true
          @exclusive = false
        end
        @parser.on("--arguments=JSON", "Queue arguments as a JSON string") do |v|
          begin
            json = JSON.parse(v)
            if args = json.as_h?
              @args = ::AMQP::Client::Arguments.new(args)
            else
              abort "Error: --arguments must be a JSON object"
            end
          rescue JSON::ParseException
            abort "Error: Invalid JSON in --arguments parameter"
          end
        end
        @parser.on("-n", "--no-wait", "Don't wait for queue declaration confirm") do
          @no_wait = true
        end
      end

      def run
        super
        count = 0
        c = ::AMQP::Client.new(@uri).connect
        ch = c.channel
        loop do
          @queues.times do
            ch.queue_declare("lavinmqperf-queue-#{Random::DEFAULT.hex(8)}",
              durable: @durable, exclusive: @exclusive, no_wait: @no_wait, args: @args)
            print '.'
          end
          puts
          print "#{count += @queues} queues "
          puts "Using #{rss.humanize_bytes} memory."
          puts "Press enter to add #{@queues} more queues or ctrl-c to abort"
          gets
        end
      end
    end
  end
end
