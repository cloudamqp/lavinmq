require "amqp-client"
require "./perf"

module LavinMQPerf
  class ChannelChurn < Perf
    def run
      super
      c = AMQP::Client.new(@uri)
      conn = c.connect
      Benchmark.ips do |x|
        x.report("open-close channel") do
          ch = conn.channel
          ch.close
        end
      end
    end
  end
end
