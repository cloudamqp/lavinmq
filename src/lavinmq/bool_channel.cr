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

    # Wake one parked receiver per activation instead of all of them, for a
    # state that means "there is work": the woken receiver takes the work,
    # and whoever adds more work while the state stays active wakes one more
    # with #signal_one. A woken receiver that can't take the work must pass
    # the wake on (#signal_one) before parking on something else, or the
    # receivers left parked never find out about it.
    property? wake_one = false

    def activate
      @lock.sync do
        @queue.as(EndlessQueue).empty = false
        if wake_one?
          wake_receiver
        else
          while wake_receiver
          end
        end
      end
    end

    # Wakes one parked receiver, if there is one and the state is active (an
    # inactive state wakes one when activated). The unlocked check is sound:
    # while the state is active no receiver parks, so any parked one was
    # there before the activation, which the caller saw.
    def signal_one : Nil
      return if @receivers.empty?
      @lock.sync do
        return if @queue.as(EndlessQueue).empty?
        wake_receiver
      end
    end

    private def wake_receiver : Bool
      receiver_ptr = dequeue_receiver || return false
      receiver_ptr.value.data = nil
      receiver_ptr.value.state = DeliveryState::Delivered
      receiver_ptr.value.fiber.enqueue
      true
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

  def close
    @when_true.close
    @when_false.close
  end
end
