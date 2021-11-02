module AvalancheMQ
  class Reporter
    def self.report(s)
      puts_size_capacity s.@users
      puts_size_capacity s.@vhosts
      s.vhosts.each do |_, vh|
        puts "VHost #{vh.name}"
        puts_size_capacity vh.@awaiting_confirm, 4
        puts_size_capacity vh.@exchanges, 4
        puts_size_capacity vh.@queues, 4
        puts_size_capacity vh.@segment_references, 4
        vh.queues.each do |_, q|
          puts "    #{q.name} #{q.durable ? "durable" : ""} args=#{q.arguments}"
          puts_size_capacity q.@consumers, 6
          puts_size_capacity q.@ready, 6
          puts_size_capacity q.@unacked, 6
          puts_size_capacity q.@requeued, 6
          puts_size_capacity q.@deliveries, 6
        end
        puts_size_capacity vh.@connections
        vh.connections.each do |c|
          puts "  #{c.name}"
          puts_size_capacity c.@channels, 4
          c.channels.each_value do |ch|
            puts "    #{ch.id} prefetch=#{ch.prefetch_size}"
            puts_size_capacity ch.@unacked, 6
            puts_size_capacity ch.@consumers, 6
            puts_size_capacity ch.@visited, 6
            puts_size_capacity ch.@found_queues, 6
          end
        end
      end
    end

    def self.print_queue_segments(amqp_server, io)
      amqp_server.vhosts.each_value do |vhost|
        vhost.queues.each_value do |q|
          s = Set(UInt32).new
          q.@unacked.each_sp {|sp| s << sp.segment }
          q.@ready.each {|sp| s << sp.segment }
          io.puts "queue=\"#{vhost.name}/#{q.name}\" segments=#{s}"
        end
      end
    end

    def self.dump_string_pool(io)
      pool = AMQ::Protocol::ShortString::POOL
      io.puts "# size=#{pool.size} capacity=#{pool.@capacity}"
      pool.@capacity.times do |i|
        str = pool.@values[i]
        next if str.empty?
        io.puts str
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
