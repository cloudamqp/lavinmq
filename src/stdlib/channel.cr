class Channel(T)
  # Sends a value to the channel if it has capacity.
  # It doesn't block if the channel doesn't have capacity.
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

  # Sends a value to the channel if it has capacity.
  # It doesn't block if the channel doesn't have capacity.
  #
  # Returns `true` if the value could be deliviered immediately, `false` otherwise, also when the channel is closed.
  def try_send?(value : T) : Bool
    @lock.lock
    case send_internal(value)
    in .delivered?
      true
    in .closed?
      false
    in .none?
      false
    end
  ensure
    @lock.unlock
  end
end
