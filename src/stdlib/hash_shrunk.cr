class Hash(K, V)
  # A Hash never releases capacity and `dup` keeps it, so this returns a
  # right sized copy when the hash is mostly empty, otherwise `self`.
  # Hash iteration fixes its upper bound when it starts, so the copy must
  # replace the original rather than shrinking it in place under a fiber
  # suspended inside `each`.
  def shrunk : self
    capacity = entries_capacity
    return self unless capacity >= 64 && capacity > @size * 4
    copy = Hash(K, V).new(@block, initial_capacity: @size)
    copy.compare_by_identity if @compare_by_identity
    each { |k, v| copy[k] = v }
    copy
  end
end
