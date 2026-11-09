require "amqp-client"
require "benchmark"
require "../perf"
require "../ticker"

module LavinMQPerf
  module AMQP
    class ChannelChurn < Perf
      @rate = 0.0

      def initialize(io : IO = STDOUT, err_io : IO = STDERR)
        super(io, err_io)
        @parser.on("-r rate", "--rate=number", "Open and close channels at this rate per second until stopped, instead of benchmarking") do |v|
          @rate = v.to_f
        end
      end

      def run(args = ARGV)
        super(args)
        c = ::AMQP::Client.new(@uri)
        conn = c.connect
        if @rate > 0
          paced(conn)
        else
          Benchmark.ips do |x|
            x.report("open-close channel") do
              ch = conn.channel
              ch.close
            end
          end
        end
      end

      # Holds the connection and opens and closes a channel on it at @rate,
      # printing the number of channels so far once a second.
      private def paced(conn)
        ticker = Ticker.new(@rate)
        count = 0_u64
        last_report = Time.instant
        loop do
          conn.channel.close
          count += 1
          ticker.tick
          if Time.instant - last_report >= 1.second
            @io.puts "Channels opened and closed: #{count}"
            last_report = Time.instant
          end
        end
      end
    end
  end
end
