module Observable
  @observers = Set(Observer).new

  def register_observer(observer : Observer)
    @observers.add(observer)
  end

  def unregister_observer(observer : Observer)
    @observers.delete(observer)
  end

  def notify_observers(event : Symbol, data : Object? = nil)
    @observers.each &.on(event, data)
  end
end

module Observer
  abstract def on(event : Symbol, data : Object?)
end
