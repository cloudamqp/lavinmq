require "amqp-client"
require "benchmark"
require "../perf"

module LavinMQPerf
  module AMQP
    class ConnectionChurn < Perf
      def run(args = ARGV)
        super(args)
        c = ::AMQP::Client.new(@uri)
        Benchmark.ips do |x|
          x.report("open-close connection and channel") do
            conn = c.connect
            conn.channel
            conn.close
          end
        end
      end
    end
  end
end
