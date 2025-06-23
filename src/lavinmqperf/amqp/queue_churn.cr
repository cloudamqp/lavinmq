require "amqp-client"
require "benchmark"
require "../perf"

module LavinMQPerf
  module AMQP
    class QueueChurn < Perf
      def run
        super

        r = Random::DEFAULT
        ::AMQP::Client.start(@uri) do |c|
          ch = c.channel

          Benchmark.ips do |x|
            x.report("create/delete transient queue") do
              q = ch.queue
            ensure
              q.delete if q
            end
            x.report("create/delete durable queue") do
              q = ch.queue("queue-churn-durable-#{r.hex(8)}")
            ensure
              q.delete if q
            end
          end
        end
      end
    end
  end
end
