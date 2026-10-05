module LavinMQ
  module HTTP
    module StatsHelpers
      # Adds logs_b to logs_a, aligned at the latest value. Only logs_a is modified.
      def add_logs!(logs_a, logs_b)
        until logs_a.size >= logs_b.size
          logs_a.unshift 0
        end
        offset = logs_a.size - logs_b.size
        logs_b.each_with_index do |v, i|
          logs_a[offset + i] += v
        end
        logs_a
      end

      private def add_logs(logs_a, logs_b)
        add_logs!(logs_a.dup, logs_b)
      end
    end
  end
end
