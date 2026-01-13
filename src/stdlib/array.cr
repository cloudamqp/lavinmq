class Array(T)
  def capacity
    @capacity
  end

  private def rewind
    root_buffer.copy_from(@buffer, @size)
    shift_buffer_by -@offset_to_buffer
  end

  # Ensures that the internal buffer has at least `capacity` elements.
  def ensure_capacity(capacity : Int32) : self
    resize_to_capacity capacity if capacity >= @size
    self
  end

  # Reduces the internal buffer to exactly fit the number of elements in the
  # array, plus `extra` elements. Returns true if compaction occurred.
  def trim_to_size(*, extra : Int32 = 0, threshold : Int32 = 2) : Bool
    raise ArgumentError.new("Negative extra capacity: #{extra}") if extra < 0

    # Only compact if capacity is significantly larger than size
    return false if @capacity <= (@size + extra) * threshold

    rewind
    resize_to_capacity(@size + extra)
    true
  end
end
