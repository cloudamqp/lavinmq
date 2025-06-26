# Memory safe counter, wrapps an Atomic, but with relaxed ordering
struct Counter(T)
  def initialize(value : T = 0)
    @atomic = Atomic(T).new(value)
  end

  # Increment the counter, returns the value before the increment
  def increment : T
    @atomic.add(1, :relaxed)
  end

  # Decrement the counter, returns the value before the decrement
  def decrement : T
    @atomic.sub(1, :relaxed)
  end

  # Add a value to the counter, returns the value before the addition
  def add(value : T) : T
    @atomic.add(value, :relaxed)
  end

  # Subtract a value from the counter, returns the value before the subtraction
  def sub(value : T) : T
    @atomic.sub(value, :relaxed)
  end

  # Set the counter to a specific value
  def set(value : T) : Nil
    @atomic.set(value, :relaxed)
  end

  # Return the value of the counter
  def get : T
    @atomic.get(:relaxed)
  end
end
