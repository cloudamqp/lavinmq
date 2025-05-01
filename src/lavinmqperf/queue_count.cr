require "amqp-client"
require "./perf"

module LavinMQPerf
  class QueueCount < Perf
    @queues = 100

    def initialize
      super
      @parser.on("-q queues", "--queues=number", "Number of queues (default 100)") do |v|
        @queues = v.to_i
      end
    end

    def run
      super
      count = 0
      c = client.connect
      ch = c.channel
      loop do
        @queues.times.each_slice(100) do |slice|
          slice.each do
            ch.queue("lavinmqperf-queue-#{Random::DEFAULT.hex(8)}", durable: false, auto_delete: true, exclusive: true)
          end
        end
        puts
        print "#{count += @queues} queues "
        puts "Using #{rss.humanize_bytes} memory."
        puts "Press enter to do add #{@queues} queues or ctrl-c to abort"
        gets
      end
    end

    private def client : AMQP::Client
      @client ||= AMQP::Client.new(@uri)
    end
  end
end
