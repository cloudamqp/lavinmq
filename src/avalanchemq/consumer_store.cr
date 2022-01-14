require "./client"

module AvalancheMQ
  class ConsumerStore
    @last_consumer_selected : Client::Channel::Consumer?
    getter size

    record ConsumerGroup, priority : Int32, consumers : Deque(Client::Channel::Consumer)

    def initialize
      @lock = Mutex.new(:checked)
      @store = Array{ConsumerGroup.new(0, Deque(Client::Channel::Consumer).new(8))}
      @size = 0_u32
    end

    def add_consumer(consumer)
      @lock.synchronize do
        idx = @store.bsearch_index { |cg| cg.priority >= consumer.priority }
        if idx
          cg = @store[idx]
          if cg.priority == consumer.priority
            cg.consumers.push(consumer)
          else
            @store.insert(idx, ConsumerGroup.new(consumer.priority, Deque{consumer}))
          end
        else
          @store.push ConsumerGroup.new(consumer.priority, Deque{consumer})
        end
        @size += 1
      end
    end

    def delete_consumer(consumer)
      @lock.synchronize do
        @last_consumer_selected = nil if consumer == @last_consumer_selected

        if idx = @store.bsearch_index { |cg| cg.priority >= consumer.priority }
          cg = @store[idx]
          if cg.priority == consumer.priority
            if cg.consumers.size == 1 && !cg.priority.zero?
              # if the group only got this consumer in it
              # then delete the whole group
              # But don't delete the default priority group 0
              @store.delete_at(idx)
            else
              cg.consumers.delete(consumer)
            end
            @size -= 1
          else
            raise "Consumer not found"
          end
        end
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
            return c
          end
        else
          if i > 0 # reuse same consumer for a while if we're delivering fast
            if last = @last_consumer_selected
              if last.accepts?
                return last
              end
            end
          end
          @store.reverse_each do |cg|
            consumers = cg.consumers
            consumers.size.times do
              c = consumers.shift
              consumers.push c
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
        @store.each { |cg| cg.consumers.each &.cancel }
        @store = Array{ConsumerGroup.new(0, Deque(Client::Channel::Consumer).new(8))}
        @size = 0_u32
      end
    end

    def clear
      @lock.synchronize do
        @store = Array{ConsumerGroup.new(0, Deque(Client::Channel::Consumer).new(8))}
        @size = 0_u32
      end
    end

    def immediate_delivery?
      @store.any? { |cg| cg.consumers.any? &.accepts? }
    end

    def first
      @store.last.consumers.first
    end

    def first?
      @store.last?.try &.consumers.first?
    end

    def empty?
      @size.zero?
    end

    def each
      @store.each { |cg| cg.consumers.each { |c| yield c } }
    end

    def to_json(builder : JSON::Builder)
      builder.array do
        each do |v|
          v.to_json(builder)
        end
      end
    end

    def capacity
      @store.capacity + @store.sum &.consumers.capacity
    end
  end
end
