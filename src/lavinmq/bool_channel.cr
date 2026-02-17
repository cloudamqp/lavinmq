# A class which has two channels, one for which a state is true
# and the other for when the state if false
class BoolChannel
  class StateChannel < Channel(Nil)
    class EndlessQueue < Deque(Nil)
      property? empty : Bool = true

      def shift(&)
        yield if empty?
      end

      def push(value : T)
        raise "Can't push to an EndlessQueue"
      end
    end

    @queue = EndlessQueue.new

    def activate
      @lock.sync do
        @queue.as(EndlessQueue).empty = false
        while receiver_ptr = dequeue_receiver
          receiver_ptr.value.data = nil
          receiver_ptr.value.state = DeliveryState::Delivered
          receiver_ptr.value.fiber.enqueue
        end
      end
    end

    def deactivate
      @lock.sync do
        @queue.as(EndlessQueue).empty = true
      end
    end
  end

  getter when_true = StateChannel.new
  getter when_false = StateChannel.new
  @value : Atomic(Bool)

  def initialize(value : Bool)
    @value = Atomic.new(value)
    if value
      @when_true.activate
    else
      @when_false.activate
    end
  end

  def value
    @value.get(:acquire)
  end

  def swap(value : Bool)
    return value if @value.swap(value) == value
    if value
      @when_false.deactivate
      @when_true.activate
    else
      @when_true.deactivate
      @when_false.activate
    end
    !value
  end

  def set(value : Bool) : Nil
    swap(value)
    nil
  end

  def close
    @when_true.close
    @when_false.close
  end
end
