require "./exchange"

module LavinMQ
  class TopicExchange < Exchange
    def initialize(*args)
      super(*args)
      @queue_binding_keys = Hash(Array(String), Set(Queue)).new do |h, k|
        h[k] = Set(Queue).new
      end
      @exchange_binding_keys = Hash(Array(String), Set(Exchange)).new do |h, k|
        h[k] = Set(Exchange).new
      end
    end

    def type : String
      "topic"
    end

    def bind(destination : Queue, routing_key, headers = nil)
      ret = @queue_bindings[BindingKey.new(routing_key, nil)].add? destination
      @queue_binding_keys[routing_key.split(".")] << destination
      after_bind(destination, routing_key, headers)
      ret
    end

    def bind(destination : Exchange, routing_key, headers = nil)
      ret = @exchange_bindings[BindingKey.new(routing_key, nil)].add? destination
      @exchange_binding_keys[routing_key.split(".")] << destination
      after_bind(destination, routing_key, headers)
      ret
    end

    def unbind(destination : Queue, routing_key, headers = nil)
      ret = @queue_bindings[BindingKey.new(routing_key, nil)].delete destination
      @queue_binding_keys[routing_key.split(".")].delete destination
      after_unbind(destination, routing_key, headers)
      ret
    end

    def unbind(destination : Exchange, routing_key, headers = nil)
      ret = @exchange_bindings[BindingKey.new(routing_key, nil)].delete destination
      @exchange_binding_keys[routing_key.split(".")].delete destination
      after_unbind(destination, routing_key, headers)
      ret
    end

    def do_queue_matches(routing_key, headers = nil, & : Queue -> _)
      matches(@queue_binding_keys, routing_key, headers) do |destination|
        yield destination.as(Queue)
      end
    end

    def do_exchange_matches(routing_key, headers = nil, & : Exchange -> _)
      matches(@exchange_binding_keys, routing_key, headers) { |e| yield e.as(Exchange) }
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def matches(binding_keys, routing_key, headers = nil, & : Queue | Exchange -> _)
      return if binding_keys.empty?

      # optimize the case where the only binding key is '#'
      if binding_keys.size == 1
        bk, qs = binding_keys.first
        if bk.size == 1
          if bk.first == "#"
            qs.each { |d| yield d }
            return
          end
        end
      end

      rk_parts = routing_key.split(".")
      binding_keys.each do |bks, dst|
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
        dst.each { |d| yield d } if ok
      end
    end
  end
end
