class Channel(T)
  # Sends a value to the channel if it has capacity.
  # If the channel has spare capacity, then `true` is returned.
  # Otherwise it returns `false` without blocking, no value will be deliviered.
  #
  # Returns `true` if the value could be deliviered immediately, `false` otherwise.
  # Raises `ClosedError` if the channel is closed or closes while waiting on a full channel.
  def try_send(value : T) : Bool
    @lock.lock
    case send_internal(value)
    in .delivered?
      true
    in .closed?
      raise ClosedError.new
    in .none?
      false
    end
  ensure
    @lock.unlock
  end
end
