module AvalancheMQ
  class ConsumerStore

    @last_consumer_selected : Client::Channel::Consumer?
    getter size

    def initialize
      @lock = Mutex.new(:unchecked)
      @store = Deque(Deque(Client::Channel::Consumer)).new
      @size = 0_u16
    end

    def add_consumer(consumer)
      @lock.synchronize do
        @size += 1
        @store.each_with_index do |consumer_group, idx|
          if consumer.priority > consumer_group.first.priority
            new_deque = Deque(Client::Channel::Consumer).new
            new_deque.push(consumer)
            @store.insert(idx, new_deque)
            return
          elsif consumer.priority == consumer_group.first.priority
            @store[idx].push(consumer)
            return
          end
        end
        new_deque = Deque(Client::Channel::Consumer).new
        new_deque.push(consumer)
        @store.push(new_deque)
      end
    end

    def delete_consumer(consumer)
      @lock.synchronize do
        @size -=1
        @last_consumer_selected = nil if consumer == @last_consumer_selected
        consumer_group = @store.bsearch do |cg|
          consumer.priority >= cg.first.priority
        end
        return consumer unless consumer_group
        if consumer_group.size == 1
          @store.delete consumer_group
        else
          consumer_group.delete consumer
        end
        return consumer
      end
    end

    def next_consumer(i)
      @lock.synchronize do
        case @size
        when 0
          nil
        when 1
          c = first
          if c.accepts?
            @last_consumer_selected = c
            c
          end
        else
          if i > 0 # reuse same consumer for a while if we're delivering fast
            return @last_consumer_selected if @last_consumer_selected.try &.accepts?
          end
          @store.each do |consumer_group|
            consumer_group.size.times do
              c = consumer_group.shift
              consumer_group.push c
              if c.accepts?
                @last_consumer_selected = c
                return c
              end
            end
          end
        end
      end
    end

    def cancel_consumers
      @lock.synchronize do
        @store.each { |consumer_group| consumer_group.each &.cancel }
      end
    end

    def clear
      @lock.synchronize { @store.clear }
    end

    def immediate_delivery?
      @store.any? { |consumer_group| consumer_group.any? &.accepts? }
    end

    def first
      @store.first.first
    end

    def first?
      @store.first?.try &.first?
    end

    def empty?
      @size == 0
    end

    def to_a
      @store.to_a
    end

    def capacity
      @store.sum &.capacity
    end
  end
end
