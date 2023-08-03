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
    in .none?
      false
    in .closed?
      raise ClosedError.new
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
    in .none?, .closed?
      false
    end
  ensure
    @lock.unlock
  end

  def try_receive : T?
    @lock.lock
    state, value = receive_internal
    case state
    in .delivered?
      raise "BUG: Unexpected UseDefault value for delivered receive" if value.is_a?(UseDefault)
      value
    in .closed?
      raise ClosedError.new
    in .none?
      nil
    end
  ensure
    @lock.unlock
  end

  def try_receive? : T?
    @lock.lock
    state, value = receive_internal
    case state
    in .delivered?
      raise "BUG: Unexpected UseDefault value for delivered receive" if value.is_a?(UseDefault)
      value
    in .none?, .closed?
      nil
    end
  ensure
    @lock.unlock
  end

  def receive(timeout : Time::Span) : T
    select
    when msg = receive
      msg
    when timeout(timeout)
      raise "Channel timeout"
    end
  end
end
