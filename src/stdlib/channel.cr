class Channel(T)
  def full?
    if queue = @queue
      return queue.size >= @capacity
    end

    !@senders.empty?
  end

  def empty?
    if queue = @queue
      return queue.empty?
    end

    @senders.empty?
  end
end
