module LavinMQ
  class Reporter
    def self.report(s)
      puts_size_capacity s.@users
      puts_size_capacity s.@vhosts
      s.vhosts.each do |_, vh|
        puts "VHost #{vh.name}"
        puts_size_capacity vh.@exchanges, 4
        puts_size_capacity vh.@queues, 4
        vh.queues.each do |_, q|
          puts "    #{q.name} #{q.durable ? "durable" : ""} args=#{q.arguments}"
          puts_size_capacity q.@consumers, 6
          puts_size_capacity q.@deliveries, 6
          puts_size_capacity q.@msg_store.@segments, 6
          puts_size_capacity q.@msg_store.@acks, 6
          puts_size_capacity q.@msg_store.@deleted, 6
          puts_size_capacity q.@msg_store.@segment_msg_count, 6
          puts_size_capacity q.@msg_store.@requeued, 6
        end
        puts_size_capacity vh.@connections
        vh.connections.each do |c|
          puts "  #{c.name}"
          puts_size_capacity c.@channels, 4
          c.channels.each_value do |ch|
            puts "    #{ch.id} global_prefetch=#{ch.global_prefetch_count} prefetch=#{ch.prefetch_count}"
            puts_size_capacity ch.@unacked, 6
            puts_size_capacity ch.@consumers, 6
            puts_size_capacity ch.@visited, 6
            puts_size_capacity ch.@found_queues, 6
          end
        end
      end
    end

    macro puts_size_capacity(obj, indent = 0)
      STDOUT << " " * {{ indent }}
      STDOUT << "{{ obj.name }}"
      STDOUT << " size="
      STDOUT << {{obj}}.size
      STDOUT << " capacity="
      STDOUT << {{obj}}.capacity
      STDOUT << '\n'
    end
  end
end
