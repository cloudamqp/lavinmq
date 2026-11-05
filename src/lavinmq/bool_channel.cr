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
  # Makes updating the value and switching the active channel one step, so
  # concurrent sets can't leave them disagreeing
  @lock = Mutex.new
  # Fibers in #wait_until_true, parked or woken but not yet resumed
  @waiting = Atomic(Int32).new(0)

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
    @lock.synchronize do
      ret = @value.swap(value)
      update_active_channel(value) unless ret == value
      ret
    end
  end

  def set(value : Bool) : Nil
    @lock.synchronize do
      @value.set(value)
      update_active_channel(value)
    end
    nil
  end

  private def update_active_channel(value)
    if value
      @when_false.deactivate
      @when_true.activate
    else
      @when_true.deactivate
      @when_false.activate
    end
  end

  # Whether any fiber is in #wait_until_true. A fiber that stays out of the
  # wait while others are in it should yield before it takes what they're
  # waiting for, or it can win every race and starve them.
  def waiting? : Bool
    @waiting.get(:relaxed) > 0
  end

  # Blocks until the value is true. Returns false if *abort* is closed or
  # receives first. All waiters are woken when the value turns true, and the
  # first one to run gets it. The others re-park directly, without yielding,
  # so they keep their place in line.
  def wait_until_true(abort : ::Channel) : Bool
    @waiting.add(1, :relaxed)
    begin
      until value
        select
        when @when_true.receive
        when abort.receive?
          return false
        end
      end
      true
    ensure
      @waiting.sub(1, :relaxed)
    end
  end

  def close
    @when_true.close
    @when_false.close
  end
end
