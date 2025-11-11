module LavinMQ
  class Reporter
    def self.report(s)
      if s.nil?
        puts "No server instance to report"
        return
      end
      puts_size_capacity s.@users
      s.users.each do |name, user|
        puts "User #{name}"
        puts_size_capacity user.@tags, 4
        puts_size_capacity user.@permissions, 4
      end
      puts_size_capacity s.@vhosts
      s.vhosts.each do |name, vh|
        puts "VHost #{name}"
        puts_size_capacity vh.@exchanges, 4
        puts_size_capacity vh.@queues, 4
        vh.queues.each do |_, q|
          puts "    #{q.name} #{q.durable? ? "durable" : ""} args=#{q.arguments}"
          if q = (q.as(LavinMQ::AMQP::Queue) || q.as(LavinMQ::MQTT::Session))
            puts_size_capacity q.@consumers, 6
            puts_size_capacity q.@deliveries, 6
            puts_size_capacity q.@msg_store.@segments, 6
            puts_size_capacity q.@msg_store.@acks, 6
            puts_size_capacity q.@msg_store.@deleted, 6
            puts_size_capacity q.@msg_store.@segment_msg_count, 6
            puts_size_capacity q.@msg_store.@requeued, 6
          end
        end
        puts_size_capacity vh.@connections
        vh.connections.each do |c|
          puts "  #{c.name}"
          puts_size_capacity c.channels, 4
          case c
          when LavinMQ::AMQP::Client
            puts_size_capacity c.@acl_write_cache, 4
          end
          c.channels.each_value do |ch|
            puts "    #{ch.id} global_prefetch=#{ch.global_prefetch_count} prefetch=#{ch.prefetch_count}"
            puts_size_capacity ch.consumers, 8
            case ch
            when AMQP::Channel
              puts_size_capacity ch.@unacked, 8
              puts_size_capacity ch.@visited, 8
              puts_size_capacity ch.@found_queues, 8
            end
          end
        end
      end
    end

    macro puts_size_capacity(obj, indent = 0)
      {{ indent }}.times do
        STDOUT << ' '
      end
      STDOUT << "{{ obj.name }}"
      STDOUT << " size="
      STDOUT << {{obj}}.size
      STDOUT << " capacity="
      STDOUT << {{obj}}.capacity
      STDOUT << '\n'
    end
  end
end
