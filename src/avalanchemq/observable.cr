module Observable
  @observers = Set(Observer).new

  def registerObserver(observer : Observer)
    @observers.add(observer)
  end

  def unregisterObserver(observer : Observer)
    @observers.delete(observer)
  end

  def notifyObservers(event : Symbol, data : Object? = nil)
    @observers.each { |o| o.on(event, data) }
  end
end

module Observer
  abstract def on(event : Symbol, data : Object?)
end
