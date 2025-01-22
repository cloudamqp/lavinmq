require "amqp-client"
require "./perf"

module LavinMQPerf
  class ConsumerChurn < Perf
    def run
      super
      c = AMQP::Client.new(@uri).connect
      ch = c.channel
      q = ch.queue_declare "", auto_delete: false
      Benchmark.ips do |x|
        x.report("open-close consumer") do
          tag = ch.basic_consume(q[:queue_name]) { }
          ch.basic_cancel(tag, no_wait: true)
        end
      end
    end
  end
end
