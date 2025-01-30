require "./exchange"

module LavinMQ
  module AMQP
    class TopicExchange < Exchange
      @bindings = Hash(Array(String), Set(LavinMQ::Destination)).new do |h, k|
        h[k] = Set(LavinMQ::Destination).new
      end

      def type : String
        "topic"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |rk, ds|
          ds.each.map do |d|
            binding_key = BindingKey.new(rk.join("."))
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : AMQP::Destination, routing_key, headers = nil)
        return false unless @bindings[routing_key.split(".")].add? destination
        binding_key = BindingKey.new(routing_key)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : AMQP::Destination, routing_key, headers = nil)
        rks = routing_key.split(".")
        bds = @bindings[routing_key.split(".")]
        return false unless bds.delete destination
        @bindings.delete(rks) if bds.empty?

        binding_key = BindingKey.new(routing_key)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      protected def bindings(routing_key : String, headers : AMQP::Table?, &)
        select_matches(routing_key) do |destination|
          yield destination
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def select_matches(routing_key, &)
        binding_keys = @bindings

        return if binding_keys.empty?

        # optimize the case where the only binding key is '#'
        if binding_keys.size == 1
          bk, qs = binding_keys.first
          if bk.size == 1
            if bk.first == "#"
              qs.each do |q|
                yield q
              end
            end
          end
        end

        rk_parts = routing_key.split(".")
        binding_keys.each do |bks, destinations|
          ok = false
          prev_hash = false
          size = bks.size # binding keys can max be 256 chars long anyway
          j = 0
          i = 0
          bks.each do |part|
            if rk_parts.size <= j
              ok = false
              break
            end
            case part
            when "#"
              j += 1
              prev_hash = true
              ok = true
            when "*"
              prev_hash = false
              # Is this the last bk and the last rk?
              if size == i + 1 && rk_parts.size == j + 1
                ok = true
                break
                # More than 1 rk left ok move on
              elsif rk_parts.size > j + 1
                j += 1
                i += 1
                next
              else
                ok = false
                j += 1
              end
            else
              if prev_hash
                if size == (i + 1)
                  ok = rk_parts.last == part
                  j += 1
                else
                  ok = false
                  rk_parts[j..-1].each do |rk_part|
                    j += 1
                    ok = part == rk_part
                    break if ok
                  end
                end
              else
                # Is this the last bk but not the last rk?
                if size == i + 1 && rk_parts.size > j + 1
                  ok = false
                else
                  ok = rk_parts[j] == part
                end
                j += 1
              end
            end
            break unless ok
            i += 1
          end
          if ok
            destinations.each do |destination|
              yield destination
            end
          end
        end
      end
    end
  end
end
