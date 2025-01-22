require "amqp-client"
require "./perf"

module LavinMQPerf
  class BindChurn < Perf
    def run
      super

      r = Random::DEFAULT
      AMQP::Client.start(@uri) do |c|
        ch = c.channel
        temp_q = ch.queue
        durable_q = ch.queue("bind-churn-durable-#{r.hex(8)}")

        Benchmark.ips do |x|
          x.report("bind non-durable queue") do
            temp_q.bind "amq.direct", r.hex(16)
          end
          x.report("bind durable queue") do
            durable_q.bind "amq.direct", r.hex(10)
          end
        end
      ensure
        durable_q.delete if durable_q
      end
    end
  end
end
