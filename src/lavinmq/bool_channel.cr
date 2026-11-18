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
  # Turn channels of the fibers in #wait_until, first in line first
  @line = Deque(::Channel(Nil)).new

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
    wake_first_in_line
  end

  # Blocks until the value is *target* and this fiber is first in line. Only
  # the first is woken, and it wakes the next as it leaves. All waiters on
  # one BoolChannel must wait for the same value, or one can block the line.
  # Returns false when aborted or after *timeout* without a turn.
  def wait_until(target : Bool, turn : ::Channel(Nil), abort : ::Channel,
                 timeout : Time::Span? = nil, & : ->) : Bool
    @lock.synchronize { @line.push turn }
    loop do
      return true if @lock.synchronize { value == target && @line.first? == turn }
      yield
      if timeout
        select
        when turn.receive
        when abort.receive?
          return false
        when timeout(timeout)
          return false
        end
      else
        select
        when turn.receive
        when abort.receive?
          return false
        end
      end
    end
  ensure
    @lock.synchronize do
      @line.first? == turn ? @line.shift : @line.delete(turn)
      wake_first_in_line if value == target
    end
  end

  private def wake_first_in_line : Nil
    if turn = @line.first?
      select
      when turn.send(nil)
      else
      end
    end
  end

  # Doesn't release fibers in #wait_until, they leave through their
  # abort channel
  def close
    @when_true.close
    @when_false.close
  end
end
