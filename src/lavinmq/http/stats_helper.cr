module LavinMQ
  module HTTP
    module StatsHelpers
      def add_logs!(logs_a, logs_b)
        until logs_a.size >= logs_b.size
          logs_a.unshift 0
        end
        until logs_b.size >= logs_a.size
          logs_b.unshift 0
        end
        logs_a.size.times do |i|
          logs_a[i] += logs_b[i]
        end
        logs_a
      end

      private def add_logs(logs_a, logs_b)
        add_logs!(logs_a.dup, logs_b)
      end
    end
  end
end
