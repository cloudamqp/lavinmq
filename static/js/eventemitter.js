class EventEmitter {
  listeners = new Map()
  emit(eventName, ...args) {
    if (!this.listeners.has(eventName)) {
      return
    }
    this.listeners.get(eventName).forEach(listener => {
      listener(...args)
    })
  }

  on(eventName, listener) {
    if (!this.listeners.has(eventName)) {
      this.listeners.set(eventName, [])
    }
    this.listeners.get(eventName).push(listener)
  }
}

export default EventEmitter
