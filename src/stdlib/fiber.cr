class Fiber
  def self.list(&blk : Fiber -> Nil)
    fibers.unsafe_each &blk
  end

  def self.count
    c = 0
    Fiber.list { |_| c += 1 }
    c
  end
end

# Expose the holder of a checked/reentrant Sync::Mutex for SIGUSR1 debug dumps.
# Unchecked mutexes don't track ownership and return nil.
class Sync::Mutex
  def locked_by_fiber : Fiber?
    @type.unchecked? ? nil : @locked_by
  end
end

class Sync::RWLock
  def locked_by_fiber : Fiber?
    @type.unchecked? ? nil : @locked_by
  end
end

class Sync::Shared(T)
  def locked_by_fiber : Fiber?
    {% if compare_versions(Crystal::VERSION, "1.12.0") >= 0 %}
      @lock.to_reference.locked_by_fiber
    {% else %}
      @lock.locked_by_fiber
    {% end %}
  end
end

class Sync::Exclusive(T)
  def locked_by_fiber : Fiber?
    {% if compare_versions(Crystal::VERSION, "1.12.0") >= 0 %}
      @lock.to_reference.locked_by_fiber
    {% else %}
      @lock.locked_by_fiber
    {% end %}
  end
end
